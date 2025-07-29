import streamlit as st
st.title("Test")
st.write("Funciona")
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
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def init_supabase():
    try:
        url = st.secrets.get("supabase", {}).get("url", "https://qzexuqkedukcwcyhrpza.supabase.co")
        key = st.secrets.get("supabase", {}).get("key", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0")
        supabase: Client = create_client(url, key)
        return supabase
    except Exception as e:
        st.error(f"Error conectando a Supabase: {str(e)}")
        return None

        
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

store_config = {
    '1-TODOENCARGO-CO': {'prefijo': 'TDC', 'pais': 'Colombia', 'tipo_calculo': 'A'},
    '2-MEGATIENDA SPA': {'prefijo': 'MEGA', 'pais': 'Chile', 'tipo_calculo': 'B'},
    '3-VEENDELO': {'prefijo': 'VEEN', 'pais': 'Colombia', 'tipo_calculo': 'B'},
    '4-MEGA TIENDAS PERUANAS': {'prefijo': 'MGA-PE', 'pais': 'Perú', 'tipo_calculo': 'A'},
    '5-DETODOPARATODOS': {'prefijo': 'DTPT', 'pais': 'Colombia', 'tipo_calculo': 'C'},
    '6-COMPRAFACIL': {'prefijo': 'CFA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
    '7-COMPRA-YA': {'prefijo': 'CPYA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
    '8-FABORCARGO': {'prefijo': 'FBC', 'pais': 'Chile', 'tipo_calculo': 'D'}
}
