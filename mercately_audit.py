#!/usr/bin/env python3
"""
mercately_audit.py
Auditoría: compara clientes en la API vs la BD para detectar registros faltantes.
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Busca el .env en el mismo directorio que este script, sin importar desde dónde se ejecute
_env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=_env_path, override=True)

# ── VALIDACIÓN DE VARIABLES ───────────────────────────────────────────────────
_required = ['PGUSER', 'PGPASSWORD', 'PGHOST', 'PGDATABASE', 'API_KEY']
_missing = [v for v in _required if not os.getenv(v)]
if _missing:
    print("❌  Faltan variables de entorno. Estado actual:")
    for v in _required:
        val = os.getenv(v)
        print(f"   {v}: {'✅ OK' if val else '❌ FALTA'}")
    print(f"\n📄  Crea el archivo .env en: {_env_path.parent}")
    print(f"    Usa .env.example como plantilla.")
    sys.exit(1)

# ── CONEXIÓN BD ───────────────────────────────────────────────────────────────
PG_PORT = os.getenv('PGPORT') or '5432'
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}"
    f"@{os.getenv('PGHOST')}:{PG_PORT}/{os.getenv('PGDATABASE')}?sslmode=require"
)
API_KEY = os.getenv('API_KEY')
CHECKPOINT_FILE = Path("mercately_checkpoint.json")

COLS_DISPLAY = ['id', 'first_name', 'last_name', 'phone', 'email', 'creation_date']


# ── CLIENTE API ───────────────────────────────────────────────────────────────
class MercatelyClient:
    def __init__(self, api_key):
        self.base_url = "https://app.mercately.com/retailers/api/v1"
        self.headers = {
            "api-key": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        }

    def get_page(self, start_date, end_date, page=1):
        params = {"page": page, "start_date": str(start_date), "end_date": str(end_date)}
        resp = requests.get(
            f"{self.base_url}/customers",
            headers=self.headers,
            params=params,
            timeout=45
        )
        if resp.status_code != 200:
            print(f"  ⚠️  API respondió {resp.status_code}: {resp.text[:200]}")
            return None
        return resp.json()

    def fetch_range(self, start_date, end_date, max_pages=300, label=""):
        """Descarga todos los clientes en un rango de fechas con paginación."""
        all_customers = []
        page = 1
        tag = f"[{label}] " if label else ""
        print(f"  {tag}Consultando API: {start_date} → {end_date}")
        while page <= max_pages:
            data = self.get_page(start_date, end_date, page)
            if not data or not data.get('customers'):
                break
            batch = data['customers']
            if not batch:
                break
            all_customers.extend(batch)
            print(f"  {tag}Página {page:>3}: {len(batch):>4} clientes  |  Acum: {len(all_customers):,}", end='\r')
            page += 1
            time.sleep(0.4)
        print()
        return all_customers


# ── SECCIÓN 1: CHECKPOINT ─────────────────────────────────────────────────────
def check_checkpoint():
    sep()
    print("SECCIÓN 1 — ESTADO DEL CHECKPOINT")
    sep()
    if not CHECKPOINT_FILE.exists():
        print("❌  No existe mercately_checkpoint.json")
        print("    El ETL nunca guardó un checkpoint o fue borrado.")
        return None

    with open(CHECKPOINT_FILE) as f:
        data = json.load(f)

    last_run = data.get('last_run')
    last_run_date = pd.to_datetime(last_run).date()
    days_ago = (datetime.now().date() - last_run_date).days

    print(f"Último run registrado : {last_run}")
    print(f"Hace                  : {days_ago} día(s)")

    if days_ago == 0:
        print("✅  El ETL corrió hoy.")
    elif days_ago <= 1:
        print("✅  El ETL corrió ayer.")
    else:
        print(f"⚠️   El ETL NO ha corrido en {days_ago} días.")
        print(f"    Los clientes creados entre {last_run_date} y hoy podrían no estar en la BD.")

    return last_run_date


# ── SECCIÓN 2: GAP AUDIT (API vs BD) ─────────────────────────────────────────
def audit_gap(client, start_date, end_date):
    sep()
    print("SECCIÓN 2 — CLIENTES EN API vs BD")
    print(f"Rango auditado: {start_date} → {end_date}")
    sep()

    customers_api = client.fetch_range(start_date, end_date, label="gap")
    print(f"\n📡  API devolvió  : {len(customers_api):,} clientes en ese rango")

    if not customers_api:
        print("ℹ️   La API no devolvió clientes para este rango.")
        return pd.DataFrame()

    df_api = pd.DataFrame(customers_api)
    api_ids = set(df_api['id'].tolist())

    with engine.connect() as conn:
        db_ids = set(
            pd.read_sql(text("SELECT id FROM mercately_clientes"), conn)['id'].tolist()
        )
        total_db = conn.execute(text("SELECT COUNT(*) FROM mercately_clientes")).scalar()

    missing_ids = api_ids - db_ids
    print(f"🗄️   BD tiene      : {total_db:,} clientes totales")
    print(f"❌  Faltantes     : {len(missing_ids):,} clientes que la API devuelve pero NO están en la BD")

    if missing_ids:
        df_missing = df_api[df_api['id'].isin(missing_ids)]
        cols = [c for c in COLS_DISPLAY if c in df_missing.columns]
        print(f"\n{'─'*60}")
        print("Clientes en API que NO están en la BD:")
        print(df_missing[cols].to_string(index=False))
        csv_path = "audit_missing_customers.csv"
        df_missing[cols].to_csv(csv_path, index=False)
        print(f"\n💾  Exportado a: {csv_path}")
    else:
        print("✅  Todos los clientes del rango ya están en la BD.")

    return df_api


# ── SECCIÓN 3: BÚSQUEDA POR TELÉFONO ─────────────────────────────────────────
def search_phone(client, phone, df_api_cache=None, days_range=90):
    sep()
    print(f"SECCIÓN 3 — BÚSQUEDA POR TELÉFONO: {phone}")
    sep()

    # Buscar en BD
    with engine.connect() as conn:
        df_db = pd.read_sql(
            text("SELECT id, first_name, last_name, phone, email, creation_date "
                 "FROM mercately_clientes WHERE phone ILIKE :phone ORDER BY creation_date DESC"),
            conn,
            params={"phone": f"%{phone}%"}
        )

    if df_db.empty:
        print(f"❌  BD : No encontrado")
    else:
        print(f"✅  BD : {len(df_db)} registro(s)")
        print(df_db.to_string(index=False))

    # Buscar en API — usa caché si ya se descargó, si no consulta de nuevo
    if df_api_cache is not None and not df_api_cache.empty:
        df_api = df_api_cache
        print(f"\n🔍  Buscando en datos de API ya descargados ({len(df_api):,} clientes)...")
    else:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_range)
        print(f"\n🔍  Buscando en API (últimos {days_range} días: {start_date} → {end_date})...")
        customers = client.fetch_range(start_date, end_date, label=f"phone:{phone}")
        df_api = pd.DataFrame(customers) if customers else pd.DataFrame()

    if df_api.empty or 'phone' not in df_api.columns:
        print("❌  API : No se pudo consultar o no hay datos")
        return

    matches = df_api[df_api['phone'].astype(str).str.contains(phone, na=False)]

    if matches.empty:
        print(f"❌  API : Teléfono {phone} NO encontrado en los datos consultados")
        print("    Posibles causas:")
        print("    a) El cliente fue creado fuera del rango de fechas auditado")
        print("    b) El campo 'phone' tiene formato distinto en la API (ej: +57...)")
        print("    c) El cliente no existe aún en Mercately")

        # Buscar variantes del teléfono
        digits_only = phone.lstrip('0').lstrip('+57').lstrip('+')
        alt_matches = df_api[df_api['phone'].astype(str).str.contains(digits_only[-8:], na=False)]
        if not alt_matches.empty:
            print(f"\n  ℹ️  Posible coincidencia parcial (últimos 8 dígitos '{digits_only[-8:]}'):")
            cols = [c for c in COLS_DISPLAY if c in alt_matches.columns]
            print(alt_matches[cols].to_string(index=False))
    else:
        cols = [c for c in COLS_DISPLAY if c in matches.columns]
        print(f"✅  API : {len(matches)} registro(s) encontrado(s)")
        print(matches[cols].to_string(index=False))

        # Verificar si están en la BD
        api_ids = set(matches['id'].tolist())
        with engine.connect() as conn:
            db_ids_check = set(
                pd.read_sql(text("SELECT id FROM mercately_clientes"), conn)['id'].tolist()
            )
        missing = api_ids - db_ids_check
        if missing:
            print(f"\n🚨  CONFIRMADO: ID(s) {missing} están en la API pero NO en la BD")
        else:
            print(f"\n✅  Todos los registros de este teléfono están en la BD.")


# ── SECCIÓN 4: RESUMEN FINAL ──────────────────────────────────────────────────
def summary():
    sep()
    print("SECCIÓN 4 — RESUMEN BD")
    sep()
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM mercately_clientes")).scalar()
        hoy = conn.execute(text(
            "SELECT COUNT(*) FROM mercately_clientes WHERE creation_date::date = CURRENT_DATE"
        )).scalar()
        ayer = conn.execute(text(
            "SELECT COUNT(*) FROM mercately_clientes WHERE creation_date::date = CURRENT_DATE - 1"
        )).scalar()
        ultimos_7 = conn.execute(text(
            "SELECT COUNT(*) FROM mercately_clientes WHERE creation_date >= CURRENT_DATE - INTERVAL '7 days'"
        )).scalar()
        ultimos_30 = conn.execute(text(
            "SELECT COUNT(*) FROM mercately_clientes WHERE creation_date >= CURRENT_DATE - INTERVAL '30 days'"
        )).scalar()

    print(f"Total acumulado   : {total:,}")
    print(f"Hoy               : {hoy:,}")
    print(f"Ayer              : {ayer:,}")
    print(f"Últimos 7 días    : {ultimos_7:,}")
    print(f"Últimos 30 días   : {ultimos_30:,}")


# ── UTILIDAD ──────────────────────────────────────────────────────────────────
def sep():
    print("\n" + "="*60)


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("="*60)
    print("  MERCATELY AUDIT")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    client = MercatelyClient(API_KEY)

    # 1. Estado del checkpoint
    last_run = check_checkpoint()

    # 2. Auditoría del gap: desde el último run hasta hoy
    end_date = datetime.now().date()
    audit_start = last_run if last_run else (end_date - timedelta(days=7))
    df_api_descargada = audit_gap(client, audit_start, end_date)

    # 3. Búsqueda de teléfonos específicos
    #    Agrega o quita teléfonos aquí según lo que necesites investigar
    PHONES_A_BUSCAR = [
        "3246805011",   # no está en BD (de hace ~6 horas)
        "3104924932",   # sí está en BD (del año pasado) — referencia
    ]
    for phone in PHONES_A_BUSCAR:
        search_phone(client, phone, df_api_cache=df_api_descargada)

    # 4. Resumen estadístico de la BD
    summary()

    sep()
    print("AUDITORÍA FINALIZADA")
    sep()
