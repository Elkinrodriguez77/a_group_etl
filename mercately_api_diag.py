#!/usr/bin/env python3
"""
mercately_api_diag.py
Diagnóstico rápido: prueba variaciones del endpoint para encontrar cuál responde.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / '.env', override=True)

API_KEY = os.getenv('API_KEY')
if not API_KEY:
    print("❌  No se encontró API_KEY en el .env")
    sys.exit(1)

HEADERS = {
    "api-key": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

today      = datetime.now().date()
week_ago   = today - timedelta(days=7)
month_ago  = today - timedelta(days=30)

def test(label, url, params=None):
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
        body = resp.json() if resp.headers.get('content-type','').startswith('application/json') else resp.text[:200]
        status_icon = "✅" if resp.status_code == 200 else ("⚠️ " if resp.status_code < 500 else "❌")
        print(f"{status_icon} [{resp.status_code}] {label}")
        if resp.status_code != 200:
            print(f"        Respuesta: {body}")
        else:
            customers = body.get('customers', []) if isinstance(body, dict) else []
            print(f"        Clientes en página 1: {len(customers)}")
            if customers:
                sample = customers[0]
                print(f"        Muestra: {sample.get('first_name','')} {sample.get('last_name','')} | {sample.get('phone','')} | {sample.get('creation_date','')}")
    except Exception as e:
        print(f"❌  [ERROR] {label}")
        print(f"        {e}")
    print()

print("=" * 65)
print("  DIAGNÓSTICO DE ENDPOINT — MERCATELY API")
print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 65)

APP  = "https://app.mercately.com/retailers/api/v1"   # dominio original (da 404)
SHOP = "https://mercately.shop/retailers/api/v1"      # override detectado en swagger (agents)

print("\n── Dominio app. vs mercately.shop (sin fechas) ───────────────\n")
test("app.mercately.com   /retailers/api/v1/customers  (sin fechas)",
     f"{APP}/customers", {"page": 1})
test("mercately.shop      /retailers/api/v1/customers  (sin fechas)",
     f"{SHOP}/customers", {"page": 1})

print("── mercately.shop con fechas — última semana ─────────────────\n")
test("mercately.shop  (últimos 7 días)",
     f"{SHOP}/customers",
     {"page": 1, "start_date": str(week_ago), "end_date": str(today)})

print("── mercately.shop con fechas — último mes ────────────────────\n")
test("mercately.shop  (últimos 30 días)",
     f"{SHOP}/customers",
     {"page": 1, "start_date": str(month_ago), "end_date": str(today)})

print("── mercately.shop — endpoint agents (el que tiene el override) ─\n")
test("mercately.shop  /retailers/api/v1/agents",
     f"{SHOP}/agents", {"page": 1})
test("app.mercately.com  /retailers/api/v1/agents  (comparación)",
     f"{APP}/agents", {"page": 1})

print("=" * 65)
print("  FIN DIAGNÓSTICO")
print("=" * 65)
