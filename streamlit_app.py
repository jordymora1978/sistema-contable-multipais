import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
import time
import re

st.set_page_config(
    page_title="Sistema Contable Multi-País",
    page_icon="🌎",
    layout="wide"
)

@st.cache_resource
def init_supabase():
    try:
        url = "https://qzexuqkedukcwcyhrpza.supabase.co"
        key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"
        supabase = create_client(url, key)
        return supabase
    except Exception as e:
        st.error(f"Error: {str(e)}")
        return None

def get_store_config():
    return {
        '1-TODOENCARGO-CO': {'prefijo': 'TDC', 'pais': 'Colombia', 'tipo_calculo': 'A'},
        '2-MEGATIENDA SPA': {'prefijo': 'MEGA', 'pais': 'Chile', 'tipo_calculo': 'B'},
        '3-VEENDELO': {'prefijo': 'VEEN', 'pais': 'Colombia', 'tipo_calculo': 'B'},
        '4-MEGA TIENDAS PERUANAS': {'prefijo': 'MGA-PE', 'pais': 'Perú', 'tipo_calculo': 'A'},
        '5-DETODOPARATODOS': {'prefijo': 'DTPT', 'pais': 'Colombia', 'tipo_calculo': 'C'},
        '6-COMPRAFACIL': {'prefijo': 'CFA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
        '7-COMPRA-YA': {'prefijo': 'CPYA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
        '8-FABORCARGO': {'prefijo': 'FBC', 'pais': 'Chile', 'tipo_calculo': 'D'}
    }

st.title("🌎 Sistema Contable Multi-País - CORREGIDO")
st.success("✅ Código base funcionando correctamente")

page = st.sidebar.selectbox("Página:", ["Inicio", "Procesar Archivos"])

if page == "Inicio":
    st.header("Sistema funcionando")
    st.write("FABORCARGO está en Chile ✅")
    
elif page == "Procesar Archivos":
    st.header("📁 Procesar Archivos")
    st.info("Próximamente: funciones de procesamiento corregidas")

    
    def to_snake_case(name):
    name = str(name)
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    return name.lower().replace(' ', '_').replace('.', '').replace('#', '').replace('-', '_')

def calcular_asignacion(account_name, serial_number, store_config):
    if pd.isna(account_name) or pd.isna(serial_number):
        return None
    account_str = str(account_name).strip()
    prefijo = store_config.get(account_str, {}).get('prefijo', '')
    if prefijo:
        return f"{prefijo}{serial_number}"
    return None

def process_drapify_file(df_drapify):
    df_drapify.columns = [to_snake_case(col) for col in df_drapify.columns]
    duplicates_count = df_drapify['order_id'].duplicated().sum()
    if duplicates_count > 0:
        st.warning(f"⚠️ {duplicates_count} duplicados eliminados")
    df_clean = df_drapify.drop_duplicates(subset=['order_id'], keep='first').copy()
    st.info(f"📊 Órdenes: {len(df_drapify)} → {len(df_clean)} únicas")
    return df_clean

def calculate_basic_fields(df, store_config, trm_data):
    df['serial'] = df.get('serial', pd.Series(dtype=str)).astype(str)
    df['order_id'] = df.get('order_id', pd.Series(dtype=str)).astype(str)
    df['account_name'] = df.get('account_name', pd.Series(dtype=str)).astype(str)
    
    df['asignacion'] = df.apply(
        lambda row: calcular_asignacion(row['account_name'], row.get('serial', ''), store_config),
        axis=1
    )
    
    df['pais'] = df['account_name'].map(lambda x: store_config.get(str(x), {}).get('pais', 'desconocido'))
    df['tipo_calculo'] = df['account_name'].map(lambda x: store_config.get(str(x), {}).get('tipo_calculo', 'A'))
    
    pais_moneda = {'Colombia': 'COP', 'Perú': 'PEN', 'Peru': 'PEN', 'Chile': 'CLP'}
    df['moneda'] = df['pais'].map(pais_moneda)
    
    df['declare_value'] = pd.to_numeric(df.get('declare_value', 0), errors='coerce').fillna(0.0)
    df['quantity'] = pd.to_numeric(df.get('quantity', 1), errors='coerce').fillna(1.0)
    df['net_real_amount'] = pd.to_numeric(df.get('net_real_amount', 0), errors='coerce').fillna(0.0)
    
    # CORRECCIÓN: costo_amazon = declare_value × quantity
    df['costo_amazon'] = df['declare_value'] * df['quantity']
    
    # Calcular meli_usd
    trm_data = {'COP': 4000.0, 'PEN': 3.8, 'CLP': 900.0}
    df['meli_usd'] = df.apply(
        lambda row: row.get('net_real_amount', 0.0) / trm_data.get(row.get('moneda', ''), 1.0)
        if pd.notna(row.get('net_real_amount')) and row.get('moneda') in trm_data else 0.0,
        axis=1
    )
    
    # Inicializar columnas
    for col in ['total_anican', 'aditional', 'utilidad_gss', 'utilidad_socio']:
        df[col] = 0.0
    
    return df
