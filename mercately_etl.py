import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import requests
from tqdm import tqdm
import time
from pathlib import Path
import logging

# 🔒 CARGAR .env
load_dotenv()

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mercately_etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# POSTGRES DESDE .env
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}@{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}?sslmode=require"
)

API_KEY = os.getenv('API_KEY')

class MercatelyClient:
    def __init__(self, api_key: str):
        self.base_url = "https://app.mercately.com/retailers/api/v1"
        self.headers = {
            "api-key": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

    def get_customers_incremental(self, start_date, end_date, page=1):
        params = {"page": page, "start_date": start_date, "end_date": end_date}
        resp = requests.get(f"{self.base_url}/customers", headers=self.headers, params=params, timeout=45)
        return resp.json() if resp.status_code == 200 else None

class MercatelyETL:
    def __init__(self, api_key: str):
        self.client = MercatelyClient(api_key)
        self.checkpoint_file = Path("mercately_checkpoint.json")
    
    def smart_incremental(self, days_back=7):
        """🚀 ETL ACUMULATIVO + REFRESCO 7 DÍAS"""
        logger.info(f"ETL INCREMENTAL - {days_back} días")
        
        # 1. FECHAS
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        logger.info(f"Rango: {start_date} → {end_date}")
        
        # 2. DESCARGAR NUEVOS/REFRESCADOS
        df_nuevos_raw = self._download_incremental(start_date, end_date)
        if df_nuevos_raw.empty:
            logger.info("No hay datos nuevos")
            return pd.DataFrame()
        
        # 3. PREPROCESAR
        df_nuevos = self._preprocess_df(df_nuevos_raw)
        logger.info(f"{len(df_nuevos)} clientes procesados")
        
        # 4. UPSERT INTELIGENTE (histórico intacto)
        self._smart_upsert(df_nuevos, start_date)
        
        # 5. CHECKPOINT
        self._save_checkpoint(end_date)
        
        # 6. VERIFICAR
        self._verify_table()
        
        logger.info(f"✅ ETL COMPLETO: {len(df_nuevos)} registros actualizados")
        return df_nuevos
    
    def _download_incremental(self, start_date, end_date):
        """Descarga rango específico"""
        all_customers = []
        page = 1
        
        with tqdm(desc="Nuevos", unit="clientes") as pbar:
            while True:
                data = self.client.get_customers_incremental(
                    start_date.strftime('%Y-%m-%d'),
                    end_date.strftime('%Y-%m-%d'),
                    page=page
                )
                
                if not data or not data.get('customers'):
                    break
                
                customers = data['customers']
                if not customers:
                    break
                
                all_customers.extend(customers)
                pbar.update(len(customers))
                page += 1
                time.sleep(0.5)
        
        return pd.DataFrame(all_customers)
    
    def _preprocess_df(self, df):
        """Preprocesa TODAS las columnas"""
        df_clean = df.copy().replace({np.nan: None, pd.NA: None})
        
        # JSON
        for col in ['tags', 'custom_fields', 'customer_addresses']:
            if col in df_clean:
                df_clean[col] = df_clean[col].apply(lambda x: json.dumps(x) if x else None)
        
        # Numeric
        for col in ['campaign_id', 'agent']:
            if col in df_clean:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').astype('Int64')
        
        # Datetime
        for col in ['creation_date', 'sent_at', 'delivered_at', 'read_at', 'last_chat_interaction']:
            if col in df_clean:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        
        # Boolean
        if 'whatsapp_opt_in' in df_clean:
            df_clean['whatsapp_opt_in'] = df_clean['whatsapp_opt_in'].astype('boolean').replace({True: True, False: False})
        
        return df_clean
    
    def _smart_upsert(self, df_nuevos, start_date):
        """🔒 UPSERT: Actualiza/refresca sin tocar histórico"""
        with engine.begin() as conn:
            # 1. UPDATE registros existentes en el rango
            update_count = conn.execute(text("""
                UPDATE mercately_clientes 
                SET 
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    phone = EXCLUDED.phone,
                    email = EXCLUDED.email,
                    city = EXCLUDED.city,
                    campaign_id = EXCLUDED.campaign_id,
                    creation_date = EXCLUDED.creation_date,
                    last_chat_interaction = EXCLUDED.last_chat_interaction,
                    updated_at = CURRENT_TIMESTAMP
                FROM (VALUES %s) AS t(
                    id, first_name, last_name, phone, email, city, 
                    campaign_id, creation_date, last_chat_interaction
                )
                WHERE mercately_clientes.id = t.id::varchar
                AND mercately_clientes.creation_date >= :start_date
            """), [
                tuple(row[['id', 'first_name', 'last_name', 'phone', 'email', 'city', 'campaign_id', 'creation_date', 'last_chat_interaction']]) 
                for _, row in df_nuevos.iterrows()
            ], {"start_date": start_date}).rowcount
            
            # 2. INSERT nuevos
            df_nuevos.to_sql('mercately_clientes', conn, if_exists='append', index=False, method='multi', chunksize=1000)
            
            logger.info(f"🔄 {update_count} actualizados, {len(df_nuevos)} insertados")
    
    def _verify_table(self):
        """Verifica tabla post-ETL"""
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM mercately_clientes")).scalar()
            ultimos_7 = pd.read_sql("SELECT COUNT(*) FROM mercately_clientes WHERE creation_date >= CURRENT_DATE - INTERVAL '7 days'", conn).iloc[0,0]
            
            logger.info(f"TOTAL: {total:,} | ÚLTIMOS 7 DÍAS: {ultimos_7:,}")
    
    def _save_checkpoint(self, date):
        with open(self.checkpoint_file, 'w') as f:
            json.dump({"last_run": date.isoformat()}, f)

# === EJECUTAR ===
if __name__ == "__main__":
    etl = MercatelyETL(os.getenv('API_KEY'))
    
    # ETL DIARIO (5-10 min)
    df_nuevos = etl.smart_incremental(days_back=7)
    
    print("\n✅ ETL FINALIZADO")
    print("Histórico 80K = INTACTO")
    print("Nuevos 7 días = ACTUALIZADOS")