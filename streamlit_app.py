import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
import time
import re

# ============================
# CONFIGURACIÓN INICIAL
# ============================

st.set_page_config(
    page_title="Sistema Contable Multi-País",
    page_icon="🌎",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado (tu CSS original)
st.markdown("""
<style>
.main-header {
    background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    padding: 2rem;
    border-radius: 1rem;
    color: white;
    text-align: center;
    margin-bottom: 2rem;
}

.metric-card {
    background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
    padding: 1.5rem;
    border-radius: 1rem;
    text-align: center;
    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    border-left: 5px solid #007bff;
}

.success-box {
    background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #28a745;
    margin: 1rem 0;
}

.info-box {
    background: linear-gradient(135deg, #d1ecf1 0%, #bee5eb 100%);
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #17a2b8;
    margin: 1rem 0;
}

.warning-box {
    background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%);
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #ffc107;
    margin: 1rem 0;
}

.formula-box {
    background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
    padding: 1.5rem;
    border-radius: 1rem;
    border-left: 5px solid #28a745;
    margin: 1rem 0;
    box-shadow: 0 2px 10px rgba(0,0,0,0.05);
}

.stButton > button {
    width: 100%;
    border-radius: 0.5rem;
    height: 3rem;
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)

# ============================
# FUNCIONES SUPABASE (TUS FUNCIONES ORIGINALES)
# ============================

@st.cache_resource
def init_supabase():
    """Inicializa conexión con Supabase"""
    try:
        url = st.secrets.get("supabase", {}).get("url", "https://qzexuqkedukcwcyhrpza.supabase.co")
        key = st.secrets.get("supabase", {}).get("key", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0")
        
        supabase: Client = create_client(url, key)
        return supabase
    except Exception as e:
        st.error(f"Error conectando a Supabase: {str(e)}")
        return None

def test_connection():
    """Prueba la conexión con Supabase"""
    try:
        supabase = init_supabase()
        if supabase:
            result = supabase.table('trm_rates').select('count').execute() 
            return True, "✅ Conectado a Supabase"
        return False, "❌ No se pudo inicializar Supabase"
    except Exception as e:
        return False, f"❌ Error de conexión: {str(e)}"

@st.cache_data(ttl=3600)
def get_trm_rates():
    """Obtiene las últimas tasas TRM"""
    try:
        supabase = init_supabase()
        if supabase:
            try:
                result = supabase.table('trm_rates').select('*').order('created_at', desc=True).execute()
            except Exception:
                result = supabase.table('trm_rates').select('*').order('id', desc=True).execute()
                
            if result.data:
                df_trm = pd.DataFrame(result.data)
                for col_date in ['date_updated', 'created_at']:
                    if col_date in df_trm.columns:
                        df_trm[col_date] = pd.to_datetime(df_trm[col_date], errors='coerce')
                
                if 'id' in df_trm.columns:
                    df_trm['id'] = pd.to_numeric(df_trm['id'], errors='coerce')
                    df_trm = df_trm.dropna(subset=['id'])

                if 'created_at' in df_trm.columns and not df_trm['created_at'].isnull().all():
                    df_trm = df_trm.sort_values(by=['currency', 'created_at'], ascending=[True, False])
                else:
                    df_trm = df_trm.sort_values(by=['currency', 'id'], ascending=[True, False])
                
                df_trm = df_trm.drop_duplicates(subset=['currency'], keep='first')
                return {row['currency']: row['rate'] for _, row in df_trm.iterrows()}
        
        return {'COP': 4000.0, 'PEN': 3.8, 'CLP': 900.0}
    except Exception as e:
        st.error(f"Error obteniendo TRM: {str(e)}")
        return {'COP': 4000.0, 'PEN': 3.8, 'CLP': 900.0}

# ============================
# FUNCIONES DE NEGOCIO SEGURAS
# ============================

def to_snake_case(name):
    """Función auxiliar para normalizar nombres de columnas a snake_case"""
    try:
        name = str(name)
        name = re.sub(r'[^a-zA-Z0-9_]', '', name)
        name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
        name = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', name)
        return name.lower().replace(' ', '_').replace('.', '').replace('#', '').replace('-', '_')
    except Exception:
        return str(name).lower()

def get_store_config_safe():
    """Configuración de tiendas con manejo seguro de errores"""
    try:
        supabase = init_supabase()
        if supabase:
            result = supabase.table('store_config').select('*').eq('activa', True).execute()
            if result.data:
                return {row['account_name']: {
                    'prefijo': row['prefijo'],
                    'pais': row['pais'],
                    'tipo_calculo': row['tipo_calculo']
                } for row in result.data}
    except Exception as e:
        st.warning(f"Usando configuración por defecto: {str(e)}")
    
    # Configuración por defecto con FABORCARGO corregido
    return {
        '1-TODOENCARGO-CO': {'prefijo': 'TDC', 'pais': 'Colombia', 'tipo_calculo': 'A'},
        '2-MEGATIENDA SPA': {'prefijo': 'MEGA', 'pais': 'Chile', 'tipo_calculo': 'B'},
        '3-VEENDELO': {'prefijo': 'VEEN', 'pais': 'Colombia', 'tipo_calculo': 'B'},
        '4-MEGA TIENDAS PERUANAS': {'prefijo': 'MGA-PE', 'pais': 'Perú', 'tipo_calculo': 'A'},
        '5-DETODOPARATODOS': {'prefijo': 'DTPT', 'pais': 'Colombia', 'tipo_calculo': 'C'},
        '6-COMPRAFACIL': {'prefijo': 'CFA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
        '7-COMPRA-YA': {'prefijo': 'CPYA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
        '8-FABORCARGO': {'prefijo': 'FBC', 'pais': 'Chile', 'tipo_calculo': 'D'}  # CORREGIDO
    }

def obtener_gss_logistica(peso_kg):
    """Obtiene valor de GSS según peso"""
    try:
        ANEXO_A = [
            (0.01, 0.50, 24.01), (0.51, 1.00, 33.09), (1.01, 1.50, 42.17), (1.51, 2.00, 51.25),
            (2.01, 2.50, 61.94), (2.51, 3.00, 71.02), (3.01, 3.50, 80.91), (3.51, 4.00, 89.99),
            (4.01, 4.50, 99.87), (4.51, 5.00, 99.87), (5.01, 5.50, 108.95), (5.51, 6.00, 117.19),
            (6.01, 6.50, 126.12), (6.51, 7.00, 135.85), (7.01, 7.50, 144.78), (7.51, 8.00, 154.52),
            (8.01, 8.50, 163.75), (8.51, 9.00, 173.18), (9.01, 9.50, 182.11), (9.51, 10.00, 191.85),
            (10.01, 10.50, 200.78), (10.51, 11.00, 207.36), (11.01, 11.50, 216.14), (11.51, 12.00, 225.73),
            (12.01, 12.50, 234.51), (12.51, 13.00, 244.09), (13.01, 13.50, 252.87), (13.51, 14.00, 262.46),
            (14.01, 14.50, 271.24), (14.51, 15.00, 280.82), (15.01, 15.50, 289.60), (15.51, 16.00, 294.54),
            (16.01, 16.50, 303.17), (16.51, 17.00, 312.60), (17.01, 17.50, 321.23), (17.51, 18.00, 330.67),
            (18.01, 18.50, 339.30), (18.51, 19.00, 348.73), (19.01, 19.50, 357.36), (19.51, 20.00, 366.80),
            (20.01, float('inf'), 373.72)
        ]
        
        if pd.isna(peso_kg) or peso_kg <= 0:
            return 0
        
        for desde, hasta, gss_value in ANEXO_A:
            if desde <= peso_kg <= hasta:
                return gss_value
        
        return 0
    except Exception:
        return 0

def calcular_asignacion(account_name, serial_number, store_config):
    """Calcula la columna Asignacion de forma segura"""
    try:
        if pd.isna(account_name) or pd.isna(serial_number):
            return None
        
        account_str = str(account_name).strip()
        prefijo = store_config.get(account_str, {}).get('prefijo', '')
        
        if prefijo:
            return f"{prefijo}{serial_number}"
        return None
    except Exception:
        return None

def process_drapify_safe(df_drapify):
    """Procesa Drapify de forma segura eliminando duplicados"""
    try:
        # Normalizar nombres de columnas
        df_drapify.columns = [to_snake_case(col) for col in df_drapify.columns]
        
        # Verificar que order_id existe
        if 'order_id' not in df_drapify.columns:
            st.error("❌ No se encontró la columna 'order_id' en el archivo Drapify")
            return None
        
        # Verificar duplicados
        duplicates_count = df_drapify['order_id'].duplicated().sum()
        if duplicates_count > 0:
            st.warning(f"⚠️ Se encontraron {duplicates_count} order_id duplicados. Se mantendrá solo la primera ocurrencia.")
        
        # Eliminar duplicados
        df_clean = df_drapify.drop_duplicates(subset=['order_id'], keep='first').copy()
        
        st.info(f"📊 Órdenes originales: {len(df_drapify)} | Órdenes únicas: {len(df_clean)}")
        
        return df_clean
    
    except Exception as e:
        st.error(f"❌ Error procesando Drapify: {str(e)}")
        return None

def calculate_basic_columns(df, store_config, trm_data):
    """Calcula columnas básicas de forma segura"""
    try:
        # Asegurar tipos de datos
        df['serial'] = df.get('serial', pd.Series(dtype=str)).astype(str)
        df['order_id'] = df.get('order_id', pd.Series(dtype=str)).astype(str)
        df['account_name'] = df.get('account_name', pd.Series(dtype=str)).astype(str)
        
        # Calcular asignación
        df['asignacion'] = df.apply(
            lambda row: calcular_asignacion(row['account_name'], row.get('serial', ''), store_config),
            axis=1
        )
        
        # Mapear país y tipo de cálculo
        df['pais'] = df['account_name'].map(
            lambda x: store_config.get(str(x), {}).get('pais', 'desconocido')
        )
        df['tipo_calculo'] = df['account_name'].map(
            lambda x: store_config.get(str(x), {}).get('tipo_calculo', 'A')
        )
        
        # Mapear moneda
        pais_moneda = {'Colombia': 'COP', 'Perú': 'PEN', 'Peru': 'PEN', 'Chile': 'CLP'}
        df['moneda'] = df['pais'].map(pais_moneda)
        
        # Convertir columnas numéricas
        numeric_cols = ['declare_value', 'quantity', 'net_real_amount', 'logistic_weight_lbs']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df.get(col, 0), errors='coerce').fillna(0.0)
        
        # CORRECCIÓN: Calcular costo_amazon correctamente
        df['costo_amazon'] = df.get('declare_value', 0) * df.get('quantity', 1)
        
        # Calcular meli_usd
        df['meli_usd'] = df.apply(
            lambda row: (row.get('net_real_amount', 0.0) / trm_data.get(row.get('moneda', ''), 1.0))
            if pd.notna(row.get('net_real_amount')) and row.get('moneda') in trm_data else 0.0,
            axis=1
        )
        
        # Inicializar columnas
        init_cols = [
            'total_anican', 'aditional', 'amt_due_cxp', 'arancel_cxp', 'iva_cxp',
            'costo_cxp', 'bodegal', 'socio_cuenta', 'impuesto_facturacion', 
            'gss_logistica', 'impuesto_gss', 'utilidad_gss', 'utilidad_socio'
        ]
        
        for col in init_cols:
            df[col] = 0.0
        
        return df
    
    except Exception as e:
        st.error(f"❌ Error calculando columnas básicas: {str(e)}")
        raise e

def apply_business_rules_safe(df):
    """Aplica reglas de negocio de forma segura"""
    try:
        # CORRECCIÓN: bodegal solo para cuentas de Chile
        cuentas_chile = ['2-MEGATIENDA SPA', '8-FABORCARGO']
        df['bodegal'] = df.apply(
            lambda row: 3.5 if (str(row['account_name']) in cuentas_chile and 
                               str(row.get('logistic_type', '')).lower() == 'xd_drop_off') else 0.0,
            axis=1
        )
        
        # CORRECCIÓN: socio_cuenta solo para cuentas específicas
        cuentas_socio = ['2-MEGATIENDA SPA', '3-VEENDELO']
        df['socio_cuenta'] = df.apply(
            lambda row: (0.0 if str(row.get('order_status_meli', '')).lower() == 'refunded' else 1.0)
            if str(row['account_name']) in cuentas_socio else 0.0,
            axis=1
        )
        
        # CORRECCIÓN: impuesto_facturacion solo para tiendas específicas
        tiendas_impuesto = ['5-DETODOPARATODOS', '6-COMPRAFACIL', '7-COMPRA-YA']
        df['impuesto_facturacion'] = df.apply(
            lambda row: (1.0 if str(row.get('order_status_meli', '')).lower() in ['approved', 'in mediation'] else 0.0)
            if str(row['account_name']) in tiendas_impuesto else 0.0,
            axis=1
        )
        
        # CORRECCIÓN: gss_logistica solo para FABORCARGO
        df['peso_kg'] = (df.get('logistic_weight_lbs', 0) * df.get('quantity', 1)) * 0.453592
        df['gss_logistica'] = df.apply(
            lambda row: obtener_gss_logistica(row['peso_kg']) if str(row['account_name']) == '8-FABORCARGO' else 0.0,
            axis=1
        )
        
        # CORRECCIÓN: impuesto_gss solo para FABORCARGO
        df['impuesto_gss'] = df.apply(
            lambda row: (row.get('arancel_cxp', 0.0) + row.get('iva_cxp', 0.0))
            if str(row['account_name']) == '8-FABORCARGO' else 0.0,
            axis=1
        )
        
        # Asegurar que costo_cxp se asigne correctamente
        df['costo_cxp'] = df.get('amt_due_cxp', 0.0)
        
        return df
    
    except Exception as e:
        st.error(f"❌ Error aplicando reglas de negocio: {str(e)}")
        raise e

def calculate_final_profits_safe(df):
    """Calcula utilidades finales de forma segura"""
    try:
        def apply_profit_calculation(row):
            try:
                tipo = row.get('tipo_calculo', 'A')
                
                meli_usd = float(row.get('meli_usd', 0.0))
                costo_amazon = float(row.get('costo_amazon', 0.0))
                total_anican = float(row.get('total_anican', 0.0))
                aditional = float(row.get('aditional', 0.0))
                costo_cxp = float(row.get('costo_cxp', 0.0))
                bodegal = float(row.get('bodegal', 0.0))
                socio_cuenta = float(row.get('socio_cuenta', 0.0))
                impuesto_facturacion = float(row.get('impuesto_facturacion', 0.0))
                gss_logistica = float(row.get('gss_logistica', 0.0))
                impuesto_gss = float(row.get('impuesto_gss', 0.0))
                amt_due_cxp = float(row.get('amt_due_cxp', 0.0))
                
                utilidad_gss_final = 0.0
                utilidad_socio_final = 0.0
                
                if tipo == 'A':
                    utilidad_gss_final = meli_usd - costo_amazon - total_anican - aditional
                elif tipo == 'B':
                    utilidad_gss_final = meli_usd - costo_cxp - costo_amazon - bodegal - socio_cuenta
                elif tipo == 'C':
                    utilidad_base_c = meli_usd - costo_amazon - total_anican - aditional - impuesto_facturacion
                    if utilidad_base_c > 7.5:
                        utilidad_socio_final = 7.5
                        utilidad_gss_final = utilidad_base_c - 7.5
                    else:
                        utilidad_socio_final = utilidad_base_c
                        utilidad_gss_final = 0.0
                elif tipo == 'D':
                    utilidad_gss_final = gss_logistica + impuesto_gss - amt_due_cxp
                
                return pd.Series([utilidad_gss_final, utilidad_socio_final], 
                                index=['utilidad_gss', 'utilidad_socio'])
            
            except Exception:
                return pd.Series([0.0, 0.0], index=['utilidad_gss', 'utilidad_socio'])
        
        df[['utilidad_gss', 'utilidad_socio']] = df.apply(apply_profit_calculation, axis=1)
        return df
    
    except Exception as e:
        st.error(f"❌ Error calculando utilidades finales: {str(e)}")
        raise e

# ============================
# INICIALIZAR SESSION STATE
# ============================

if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None

if 'trm_data' not in st.session_state:
    st.session_state.trm_data = get_trm_rates()
    st.session_state.trm_data['last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if 'store_config' not in st.session_state:
    st.session_state.store_config = get_store_config_safe()

# ============================
# INTERFAZ PRINCIPAL
# ============================

# Header
st.markdown("""
<div class="main-header">
    <h1>🌎 Sistema Contable Multi-País</h1>
    <p>Versión Mejorada con Manejo de Errores</p>
</div>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.title("🎛️ Panel de Control")

# Estado de conexión
st.sidebar.markdown("### 🔗 Estado de Conexión")
try:
    connected, message = test_connection()
    if connected:
        st.sidebar.success(message)
    else:
        st.sidebar.error(message)
except Exception as e:
    st.sidebar.error(f"Error verificando conexión: {str(e)}")

# Navegación
page = st.sidebar.selectbox("Selecciona una sección:", [
    "🏠 Inicio",
    "📁 Procesar Archivos",
    "💱 Configurar TRM"
])

# TRM actual
st.sidebar.markdown("### 💱 TRM Actual")
try:
    for currency, rate in st.session_state.trm_data.items():
        if currency != 'last_update':
            st.sidebar.metric(f"{currency}/USD", f"{rate:,.2f}")
except Exception as e:
    st.sidebar.error(f"Error mostrando TRM: {str(e)}")

# ============================
# PÁGINA DE PROCESAMIENTO SEGURA
# ============================

if page == "📁 Procesar Archivos":
    st.header("📁 Cargar y Procesar Archivos")
    
    st.markdown("""
    <div class="info-box">
        <h4>🔄 Proceso de Carga Mejorado</h4>
        <p>Esta versión incluye manejo de errores robusto y correcciones en las reglas de negocio.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Uploaders
    col_drapify, col_otros = st.columns(2)
    
    with col_drapify:
        st.markdown("### 📊 Archivo Principal: DRAPIFY")
        drapify_file = st.file_uploader(
            "📄 Cargar DRAPIFY (Orders_XXXXXXXX)",
            type=['xlsx', 'xls'],
            key="drapify_uploader",
            help="Archivo principal con todas las órdenes de MercadoLibre."
        )
    
    with col_otros:
        st.markdown("### 🚚 Archivos Adicionales")
        anican_file = st.file_uploader(
            "🚚 Cargar Anican Logistics",
            type=['xlsx', 'xls'],
            key="anican_uploader",
            help="Archivo con costos logísticos de Anican."
        )
        aditionals_file = st.file_uploader(
            "➕ Cargar Anican Aditionals",
            type=['xlsx', 'xls'],
            key="aditionals_uploader",
            help="Archivo con costos adicionales de Anican."
        )
        cxp_file = st.file_uploader(
            "🇨🇱 Cargar Chile Express (CXP)",
            type=['xlsx', 'xls'],
            key="cxp_uploader",
            help="Archivo de Chile Express con costos logísticos y de aduana."
        )
    
    # Botón de procesamiento
    if st.button("🚀 Procesar Archivos (Versión Segura)", type="primary"):
        if drapify_file:
            try:
                with st.spinner("Procesando archivos de forma segura..."):
                    
                    # 1. Procesar Drapify
                    st.info("🔄 Paso 1: Procesando archivo Drapify...")
                    df_drapify = pd.read_excel(drapify_file)
                    df_processed = process_drapify_safe(df_drapify)
                    
                    if df_processed is None:
                        st.stop()
                    
                    # 2. Calcular columnas básicas
                    st.info("🔄 Paso 2: Calculando columnas básicas...")
                    df_processed = calculate_basic_columns(
                        df_processed, 
                        st.session_state.store_config, 
                        st.session_state.trm_data
                    )
                    
                    # 3. Procesar archivos adicionales (opcional)
                    if anican_file:
                        st.info("🔄 Paso 3a: Procesando Anican Logistics...")
                        try:
                            df_anican = pd.read_excel(anican_file)
                            df_anican.columns = [to_snake_case(col) for col in df_anican.columns]
                            
                            # Buscar columnas por patrón
                            ref_col = next((col for col in df_anican.columns if 'ref' in col.lower()), None)
                            total_col = next((col for col in df_anican.columns if 'total' in col.lower()), None)
                            
                            if ref_col and total_col:
                                df_anican[ref_col] = df_anican[ref_col].astype(str).str.strip()
                                df_anican[total_col] = pd.to_numeric(df_anican[total_col], errors='coerce').fillna(0.0)
                                
                                anican_merge = df_anican[[ref_col, total_col]].rename(
                                    columns={ref_col: 'order_id', total_col: 'total_anican_merge'}
                                )
                                
                                df_processed = df_processed.merge(anican_merge, on='order_id', how='left')
                                df_processed['total_anican'] = df_processed['total_anican_merge'].fillna(0.0)
                                df_processed = df_processed.drop(columns=['total_anican_merge'], errors='ignore')
                                
                                matches = df_processed['total_anican'].gt(0).sum()
                                st.success(f"✅ Anican Logistics: {matches} coincidencias encontradas")
                            else:
                                st.warning("⚠️ No se encontraron columnas esperadas en Anican Logistics")
                        except Exception as e:
                            st.error(f"❌ Error procesando Anican Logistics: {str(e)}")
                    
                    if aditionals_file:
                        st.info("🔄 Paso 3b: Procesando Anican Aditionals...")
                        try:
                            df_aditionals = pd.read_excel(aditionals_file)
                            df_aditionals.columns = [to_snake_case(col) for col in df_aditionals.columns]
                            
                            orderid_col = next((col for col in df_aditionals.columns if 'order' in col.lower() and 'id' in col.lower()), None)
                            qty_col = next((col for col in df_aditionals.columns if 'qty' in col.lower() or 'quantity' in col.lower()), None)
                            price_col = next((col for col in df_aditionals.columns if 'price' in col.lower() or 'unit' in col.lower()), None)
                            
                            if orderid_col and qty_col and price_col:
                                df_aditionals[orderid_col] = df_aditionals[orderid_col].astype(str).str.strip()
                                df_aditionals[qty_col] = pd.to_numeric(df_aditionals[qty_col], errors='coerce').fillna(0.0)
                                df_aditionals[price_col] = pd.to_numeric(df_aditionals[price_col], errors='coerce').fillna(0.0)
                                
                                df_aditionals['aditional_line_total'] = df_aditionals[qty_col] * df_aditionals[price_col]
                                
                                aditionals_grouped = df_aditionals.groupby(orderid_col)['aditional_line_total'].sum().reset_index()
                                aditionals_grouped = aditionals_grouped.rename(
                                    columns={orderid_col: 'order_id', 'aditional_line_total': 'aditional_merge'}
                                )
                                
                                df_processed = df_processed.merge(aditionals_grouped, on='order_id', how='left')
                                df_processed['aditional'] = df_processed['aditional_merge'].fillna(0.0)
                                df_processed = df_processed.drop(columns=['aditional_merge'], errors='ignore')
                                
                                matches = df_processed['aditional'].gt(0).sum()
                                st.success(f"✅ Anican Aditionals: {matches} coincidencias encontradas")
                            else:
                                st.warning("⚠️ No se encontraron columnas esperadas en Anican Aditionals")
                        except Exception as e:
                            st.error(f"❌ Error procesando Anican Aditionals: {str(e)}")
                    
                    if cxp_file:
                        st.info("🔄 Paso 3c: Procesando Chile Express (CXP)...")
                        try:
                            excel_file_cxp = pd.ExcelFile(cxp_file)
                            df_cxp = None
                            
                            for sheet_name in excel_file_cxp.sheet_names:
                                for header_row in range(5):
                                    try:
                                        df_test_cxp = pd.read_excel(cxp_file, sheet_name=sheet_name, header=header_row)
                                        df_test_cxp.columns = [to_snake_case(col) for col in df_test_cxp.columns]
                                        
                                        ref_col = next((col for col in df_test_cxp.columns if 'ref' in col.lower()), None)
                                        amt_col = next((col for col in df_test_cxp.columns if 'amt' in col.lower() and 'due' in col.lower()), None)
                                        arancel_col = next((col for col in df_test_cxp.columns if 'arancel' in col.lower()), None)
                                        iva_col = next((col for col in df_test_cxp.columns if 'iva' in col.lower()), None)
                                        
                                        if ref_col and amt_col:
                                            df_cxp = df_test_cxp[[ref_col, amt_col]].copy()
                                            df_cxp = df_cxp.rename(columns={ref_col: 'asignacion_cxp', amt_col: 'amt_due_cxp'})
                                            
                                            df_cxp['asignacion_cxp'] = df_cxp['asignacion_cxp'].astype(str).str.strip()
                                            df_cxp['amt_due_cxp'] = pd.to_numeric(df_cxp['amt_due_cxp'], errors='coerce').fillna(0.0)
                                            
                                            if arancel_col:
                                                df_cxp['arancel_cxp'] = pd.to_numeric(df_test_cxp[arancel_col], errors='coerce').fillna(0.0)
                                            else:
                                                df_cxp['arancel_cxp'] = 0.0
                                                
                                            if iva_col:
                                                df_cxp['iva_cxp'] = pd.to_numeric(df_test_cxp[iva_col], errors='coerce').fillna(0.0)
                                            else:
                                                df_cxp['iva_cxp'] = 0.0
                                            
                                            st.info(f"✅ CXP: Columnas detectadas en '{sheet_name}', fila {header_row}")
                                            break
                                            
                                    except Exception:
                                        continue
                                        
                                if df_cxp is not None:
                                    break
                            
                            if df_cxp is not None:
                                df_processed = df_processed.merge(
                                    df_cxp,
                                    left_on='asignacion',
                                    right_on='asignacion_cxp',
                                    how='left'
                                )
                                
                                df_processed = df_processed.drop(columns=['asignacion_cxp'], errors='ignore')
                                
                                for col in ['amt_due_cxp', 'arancel_cxp', 'iva_cxp']:
                                    if col not in df_processed.columns:
                                        df_processed[col] = 0.0
                                    else:
                                        df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0.0)
                                
                                matches = df_processed['amt_due_cxp'].gt(0).sum()
                                st.success(f"✅ Chile Express (CXP): {matches} coincidencias encontradas")
                            else:
                                st.warning("⚠️ No se pudieron detectar las columnas esperadas en el archivo CXP")
                                
                        except Exception as e:
                            st.error(f"❌ Error procesando Chile Express (CXP): {str(e)}")
                    
                    # 4. Aplicar reglas de negocio
                    st.info("🔄 Paso 4: Aplicando reglas de negocio...")
                    df_processed = apply_business_rules_safe(df_processed)
                    
                    # 5. Calcular utilidades finales
                    st.info("🔄 Paso 5: Calculando utilidades finales...")
                    df_processed = calculate_final_profits_safe(df_processed)
                    
                    # Guardar en session state
                    st.session_state.processed_data = df_processed
                    
                    # Mostrar resumen exitoso
                    st.success("✅ ¡Procesamiento completado exitosamente!")
                    
                    # Estadísticas
                    st.markdown("### 📊 Resumen de Procesamiento")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Órdenes Procesadas", len(df_processed))
                    with col2:
                        utilidad_gss_sum = df_processed['utilidad_gss'].sum()
                        st.metric("Utilidad GSS Total", f"${utilidad_gss_sum:,.2f}")
                    with col3:
                        utilidad_socio_sum = df_processed['utilidad_socio'].sum()
                        st.metric("Utilidad Socio Total", f"${utilidad_socio_sum:,.2f}")
                    with col4:
                        tiendas_count = df_processed['account_name'].nunique()
                        st.metric("Tiendas Únicas", tiendas_count)
                    
                    # Verificaciones de reglas de negocio
                    st.markdown("### ✅ Verificación de Reglas de Negocio")
                    
                    verification_results = []
                    
                    # Verificar FABORCARGO país
                    try:
                        faborcargo_data = df_processed[df_processed['account_name'] == '8-FABORCARGO']
                        if len(faborcargo_data) > 0:
                            faborcargo_pais = faborcargo_data['pais'].iloc[0]
                            if faborcargo_pais == "Chile":
                                verification_results.append("✅ FABORCARGO correctamente asignado a Chile")
                            else:
                                verification_results.append(f"❌ FABORCARGO incorrectamente asignado a: {faborcargo_pais}")
                        else:
                            verification_results.append("ℹ️ No se encontraron registros de FABORCARGO")
                    except Exception:
                        verification_results.append("⚠️ Error verificando país de FABORCARGO")
                    
                    # Verificar bodegal solo en Chile
                    try:
                        bodegal_chile = df_processed[df_processed['pais'] == 'Chile']['bodegal'].gt(0).sum()
                        bodegal_otros = df_processed[df_processed['pais'] != 'Chile']['bodegal'].gt(0).sum()
                        
                        if bodegal_otros == 0:
                            verification_results.append(f"✅ Bodegal aplicado correctamente solo en Chile ({bodegal_chile} registros)")
                        else:
                            verification_results.append(f"⚠️ Bodegal aplicado incorrectamente en otros países ({bodegal_otros} registros)")
                    except Exception:
                        verification_results.append("⚠️ Error verificando regla de bodegal")
                    
                    # Verificar GSS logística solo para FABORCARGO
                    try:
                        gss_faborcargo = df_processed[df_processed['account_name'] == '8-FABORCARGO']['gss_logistica'].gt(0).sum()
                        gss_otros = df_processed[df_processed['account_name'] != '8-FABORCARGO']['gss_logistica'].gt(0).sum()
                        
                        if gss_otros == 0:
                            verification_results.append(f"✅ GSS_logistica aplicado correctamente solo a FABORCARGO ({gss_faborcargo} registros)")
                        else:
                            verification_results.append(f"⚠️ GSS_logistica aplicado incorrectamente a otras cuentas ({gss_otros} registros)")
                    except Exception:
                        verification_results.append("⚠️ Error verificando regla de GSS logística")
                    
                    # Mostrar resultados de verificación
                    for result in verification_results:
                        if "✅" in result:
                            st.success(result)
                        elif "❌" in result:
                            st.error(result)
                        elif "⚠️" in result:
                            st.warning(result)
                        else:
                            st.info(result)
                    
                    # Vista previa de datos
                    st.markdown("### 👀 Vista Previa de Datos Procesados")
                    preview_cols = [
                        'order_id', 'account_name', 'pais', 'tipo_calculo', 
                        'meli_usd', 'costo_amazon', 'total_anican', 'aditional',
                        'costo_cxp', 'utilidad_gss', 'utilidad_socio'
                    ]
                    available_cols = [col for col in preview_cols if col in df_processed.columns]
                    
                    st.dataframe(
                        df_processed[available_cols].head(10), 
                        use_container_width=True
                    )
                    
                    # Distribuciones
                    st.markdown("### 🌍 Distribuciones")
                    col_dist1, col_dist2 = st.columns(2)
                    
                    with col_dist1:
                        st.write("**Por País:**")
                        pais_dist = df_processed['pais'].value_counts()
                        for pais, count in pais_dist.items():
                            st.write(f"• {pais}: {count} órdenes")
                    
                    with col_dist2:
                        st.write("**Por Tipo de Cálculo:**")
                        tipo_dist = df_processed['tipo_calculo'].value_counts()
                        for tipo, count in tipo_dist.items():
                            st.write(f"• Tipo {tipo}: {count} órdenes")
                    
                    # Descarga
                    try:
                        csv = df_processed.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Descargar Datos Procesados (CSV)",
                            data=csv,
                            file_name=f"datos_procesados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                        )
                    except Exception as e:
                        st.warning(f"No se pudo generar el archivo de descarga: {str(e)}")
                    
            except Exception as e:
                st.error(f"❌ Error durante el procesamiento: {str(e)}")
                st.exception(e)
        else:
            st.warning("⚠️ Por favor, carga el archivo DRAPIFY para iniciar el procesamiento.")

elif page == "🏠 Inicio":
    st.header("🏠 Bienvenido al Sistema Contable Mejorado")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <h3>📁 Procesar</h3>
            <p>Versión mejorada con manejo robusto de errores y correcciones aplicadas</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <h3>✅ Verificaciones</h3>
            <p>Sistema automático de verificación de reglas de negocio</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card">
            <h3>🔧 Correcciones</h3>
            <p>Todas las mejoras solicitadas han sido implementadas</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Lista de mejoras implementadas
    st.markdown("### 🎯 Mejoras Implementadas")
    
    mejoras = [
        "✅ Deduplicación automática por order_id en archivo Drapify",
        "✅ Corrección de costo_amazon = Declare Value × quantity",
        "✅ Mejora en detección de columnas Total Anican y Aditional",
        "✅ Corrección del país de FABORCARGO a Chile",
        "✅ Corrección de costo_cxp = Amt. Due del archivo CXP",
        "✅ impuesto_facturacion solo para tiendas específicas (COMPRAFACIL, DETODOPARATODOS, COMPRA-YA)",
        "✅ gss_logistica e impuesto_gss solo para FABORCARGO",
        "✅ socio_cuenta solo para MEGATIENDA SPA y VEENDELO",
        "✅ bodegal solo para cuentas de Chile",
        "✅ Sistema de verificaciones automáticas",
        "✅ Manejo robusto de errores"
    ]
    
    for mejora in mejoras:
        st.markdown(mejora)

elif page == "💱 Configurar TRM":
    st.header("💱 Configuración de Tasas de Cambio")
    
    st.info("🔄 Esta sección utiliza las funciones originales de TRM del código base.")
    
    # Aquí puedes incluir la funcionalidad original de TRM
    st.markdown("""
    <div class="info-box">
        <h4>ℹ️ Funcionalidad TRM</h4>
        <p>Para implementar completamente esta sección, incluye las funciones save_trm_rates y el formulario correspondiente del código original.</p>
    </div>
    """, unsafe_allow_html=True)
