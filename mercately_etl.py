import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine, text, inspect
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
# Validar variables críticas antes de conectar
required_vars = ['PGUSER', 'PGPASSWORD', 'PGHOST', 'PGDATABASE', 'API_KEY']
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    # Imprimir para debug en logs (ocultando valores reales)
    print("🔍 Estado de variables de entorno:")
    for var in required_vars:
        val = os.getenv(var)
        status = "✅ OK" if val else "❌ FALTA"
        print(f"   {var}: {status}")
    raise ValueError(f"❌ Faltan variables de entorno críticas: {', '.join(missing_vars)}")

# Manejo robusto del puerto: si es None o vacío (''), usa 5432
PG_PORT = os.getenv('PGPORT')
if not PG_PORT:
    PG_PORT = '5432'

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}@{os.getenv('PGHOST')}:{PG_PORT}/{os.getenv('PGDATABASE')}?sslmode=require"
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

    def get_customers_incremental(self, start_date, end_date, page=1, max_retries=4):
        """Clientes por rango de fechas.
        Reintenta ante errores temporales (timeout, 429, 5xx). Si la API sigue fallando
        lanza excepción: así el run falla visiblemente en vez de terminar 'exitoso' con datos incompletos."""
        params = {"page": page, "start_date": start_date, "end_date": end_date}
        for intento in range(1, max_retries + 1):
            try:
                resp = requests.get(
                    f"{self.base_url}/customers",
                    headers=self.headers,
                    params=params,
                    timeout=45
                )
            except requests.RequestException as e:
                error = f"{type(e).__name__}: {e}"
            else:
                if resp.status_code == 200:
                    return resp.json()
                error = f"HTTP {resp.status_code}: {resp.text[:300]}"
                if resp.status_code != 429 and resp.status_code < 500:
                    break  # 401/403/404...: reintentar no sirve
            if intento < max_retries:
                espera = 5 * 2 ** (intento - 1)
                logger.warning(f"⚠️ API página {page} falló ({error}) — reintento {intento}/{max_retries - 1} en {espera}s")
                time.sleep(espera)
        raise RuntimeError(f"❌ API Mercately falló en página {page}: {error}")

class MercatelyETL:
    def __init__(self, api_key: str):
        self.client = MercatelyClient(api_key)
        self.checkpoint_file = Path("mercately_checkpoint.json")
    
    def incremental_accumulate(self, days_back=90):
        """🚀 SOLO CLIENTES NUEVOS - TOTAL SIEMPRE ESTABLE"""
        print(f"🔥 ETL ACUMULATIVO")

        # Fechas: usa checkpoint si existe, si no usa days_back como fallback
        end_date = datetime.now().date()
        last_run = self._load_checkpoint()
        if last_run:
            start_date = last_run
            print(f"📂 Checkpoint encontrado — continuando desde {last_run}")
        else:
            start_date = end_date - timedelta(days=days_back)
            print(f"🆕 Sin checkpoint previo — usando days_back={days_back}")
        print(f"📅 Rango: {start_date} → {end_date}")
        
        # 1. OBTENER IDs EXISTENTES (1 seg)
        print("🔍 Verificando IDs existentes...")
        with engine.connect() as conn:
            existing_ids = set(
                pd.read_sql(text("SELECT id FROM mercately_clientes"), conn)['id'].tolist()
            )
        print(f"📊 IDs existentes: {len(existing_ids):,}")
        
        all_customers = []
        page = 1
        
        with tqdm(desc="Procesando API", unit="clientes") as pbar:
            while True:
                data = self.client.get_customers_incremental(
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d'),
                    page=page
                )
                
                if not data or not data.get('customers'):
                    print(f"\n✅ Fin datos - página {page}")
                    break
                
                customers = data['customers']
                if not customers:
                    break
                
                # ✅ FILTRAR SOLO NUEVOS ANTES DE ACUMULAR
                nuevos_customers = [c for c in customers if c.get('id') not in existing_ids]
                all_customers.extend(nuevos_customers)
                
                total_api = len(all_customers)
                pbar.update(len(customers))
                pbar.set_postfix({
                    'Página': page,
                    'Solo_nuevos': len(nuevos_customers),
                    'Total_nuevos': f"{total_api:,}"
                })
                
                page += 1
                time.sleep(0.5)
        
        df_nuevos = pd.DataFrame(all_customers)
        
        if len(df_nuevos) == 0:
            print("✅ ¡NO HAY CLIENTES NUEVOS! Total estable.")
            self._verify_accumulation()
            self._save_checkpoint(end_date)
            return df_nuevos
        
        print(f"\n🎉 {len(df_nuevos):,} CLIENTES VERDADERAMENTE NUEVOS encontrados")
        print(f"📊 Shape: {df_nuevos.shape}")
        
        # 🔒 ACUMULAR SOLO NUEVOS
        self._accumulate_safe(df_nuevos)
        self._save_checkpoint(end_date)
        
        # ANÁLISIS
        print("\n" + "="*80)
        cols_key = [c for c in ['first_name', 'last_name', 'phone', 'email', 'city', 'campaign_id', 'creation_date']
                    if c in df_nuevos.columns]
        print("📋 Primeros 10 nuevos:")
        print(df_nuevos[cols_key].head(10))
        
        self._verify_accumulation()
        return df_nuevos
    
    def _accumulate_safe(self, df_nuevos):
        """🔒 APPEND + DEDUPE ULTRA SEGURO"""
        df_clean = self._preprocess_df(df_nuevos)
        nuevos_insertados = len(df_clean)
        
        with engine.begin() as conn:
            # 0. SOLO COLUMNAS QUE EXISTEN EN LA TABLA (la API puede agregar campos nuevos)
            table_cols = {c['name'] for c in inspect(conn).get_columns('mercately_clientes')}
            extra_cols = [c for c in df_clean.columns if c not in table_cols]
            if extra_cols:
                print(f"⚠️ Columnas nuevas en la API que NO existen en la tabla (se omiten): {extra_cols}")
                df_clean = df_clean.drop(columns=extra_cols)

            # 1. CONTAR ANTES
            total_antes = conn.execute(text("SELECT COUNT(*) FROM mercately_clientes")).scalar()
            print(f"📊 TOTAL ANTES: {total_antes:,}")
            
            # 2. INSERTAR SOLO NUEVOS
            df_clean.to_sql('mercately_clientes', conn, if_exists='append', 
                           index=False, method='multi', chunksize=1000)
            
            # 3. DEDUPE FINAL (por si acaso) — conserva 1 fila por id (la más reciente)
            #    Se borra por ctid (fila física); borrar por id eliminaba TODAS las copias
            dedupe_sql = text("""
                WITH ranked AS (
                    SELECT ctid,
                           ROW_NUMBER() OVER (
                               PARTITION BY id
                               ORDER BY creation_date DESC NULLS LAST
                           ) as rn
                    FROM mercately_clientes
                )
                DELETE FROM mercately_clientes
                WHERE ctid IN (SELECT ctid FROM ranked WHERE rn > 1)
            """)
            deleted = conn.execute(dedupe_sql).rowcount
            
            # 4. CONTAR DESPUÉS
            total_despues = conn.execute(text("SELECT COUNT(*) FROM mercately_clientes")).scalar()
            
            print(f"✅ ➕ {nuevos_insertados} insertados | 🗑️ {deleted} duplicados")
            print(f"📈 FINAL: {total_antes:,} → {total_despues:,} (+{total_despues-total_antes:,})")
            
            if total_despues < total_antes:
                print("🚨 ERROR CRÍTICO: TOTAL DISMINUYÓ")
    
    def _preprocess_df(self, df):
        """Preprocesa TODAS las columnas"""
        df_clean = df.copy().replace({np.nan: None, pd.NA: None})

        # JSON: columnas conocidas + cualquier columna nueva que traiga listas/dicts
        json_cols = {'tags', 'custom_fields', 'customer_addresses', 'agent', 'inbox_chats'}
        json_cols |= {
            col for col in df_clean.columns
            if df_clean[col].apply(lambda x: isinstance(x, (list, dict))).any()
        }
        for col in json_cols:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].apply(lambda x: json.dumps(x) if x else None)
        
        # Numeric
        for col in ['campaign_id']:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').astype('Int64')
        
        # Datetime
        for col in ['creation_date', 'sent_at', 'delivered_at', 'read_at', 'last_chat_interaction']:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        
        # Boolean
        if 'whatsapp_opt_in' in df_clean.columns:
            df_clean['whatsapp_opt_in'] = df_clean['whatsapp_opt_in'].astype('boolean')
        
        return df_clean
    
    def _verify_accumulation(self):
        """Verifica total estable"""
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM mercately_clientes")).scalar()
            ultimos_7 = pd.read_sql(
                text("SELECT COUNT(*) FROM mercately_clientes WHERE creation_date >= CURRENT_DATE - INTERVAL '7 days'"), 
                conn
            ).iloc[0,0]
            
            print(f"\n🎯 TOTAL ACUMULADO: {total:,}")
            print(f"📅 ÚLTIMOS 7 DÍAS: {ultimos_7:,}")
    
    def _load_checkpoint(self):
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file) as f:
                data = json.load(f)
                return pd.to_datetime(data['last_run']).date()
        return None
    
    def _save_checkpoint(self, date):
        with open(self.checkpoint_file, 'w') as f:
            json.dump({"last_run": date.isoformat()}, f)

# === EJECUTAR ===
if __name__ == "__main__":
    etl = MercatelyETL(API_KEY)
    df_nuevos = etl.incremental_accumulate(days_back=90)
    
    print("\n🎉 ETL TERMINADO")
    print("✅ TOTAL SIEMPRE ESTABLE")
    print("✅ Solo inserta VERDADERAMENTE nuevos")