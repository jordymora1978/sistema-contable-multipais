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

# CSS personalizado
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
# FUNCIONES SUPABASE
# ============================

@st.cache_resource
def init_supabase():
    """Inicializa conexión con Supabase"""
    try:
        # Intenta cargar desde st.secrets.
        # Si estas líneas causan un KeyError, significa que st.secrets no está configurado.
        # En ese caso, se usarán las claves directamente codificadas (menos seguro para producción).
        url = st.secrets.get("supabase", {}).get("url", "https://qzexuqkedukcwcyhrpza.supabase.co")
        key = st.secrets.get("supabase", {}).get("key", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0")
        
        supabase: Client = create_client(url, key)
        return supabase
    except Exception as e:
        st.error(f"Error conectando a Supabase: {str(e)}. Por favor, verifica tus credenciales y configuración.")
        return None

def test_connection():
    """Prueba la conexión con Supabase"""
    try:
        supabase = init_supabase()
        if supabase:
            # Intenta una consulta simple a una tabla que se espera que exista, como 'store_config'
            # O cualquier otra tabla pequeña que siempre esté disponible.
            result = supabase.table('trm_rates').select('count').execute() 
            return True, "✅ Conectado a Supabase"
        return False, "❌ No se pudo inicializar Supabase"
    except Exception as e:
        return False, f"❌ Error de conexión: {str(e)}"

@st.cache_data(ttl=3600) # Cache por 1 hora
def get_store_config():
    """Obtiene configuración de tiendas desde Supabase"""
    try:
        supabase = init_supabase()
        if supabase:
            # Asegúrate que 'store_config' es el nombre correcto de tu tabla en Supabase
            result = supabase.table('store_config').select('*').eq('activa', True).execute()
            if result.data:
                return {row['account_name']: {
                    'prefijo': row['prefijo'],
                    'pais': row['pais'],
                    'tipo_calculo': row['tipo_calculo']
                } for row in result.data}
        return {} # Retorna un diccionario vacío si no hay configuración o hay error
    except Exception as e:
        st.error(f"Error obteniendo configuración de tiendas desde Supabase: {str(e)}")
        # Retorna una configuración de fallback para no romper la app si Supabase falla
        return {
            '1-TODOENCARGO-CO': {'prefijo': 'TDC', 'pais': 'Colombia', 'tipo_calculo': 'A'},
            '2-MEGATIENDA SPA': {'prefijo': 'MEGA', 'pais': 'Chile', 'tipo_calculo': 'B'},
            '3-VEENDELO': {'prefijo': 'VEEN', 'pais': 'Colombia', 'tipo_calculo': 'B'},
            '4-MEGA TIENDAS PERUANAS': {'prefijo': 'MGA-PE', 'pais': 'Perú', 'tipo_calculo': 'A'},
            '5-DETODOPARATODOS': {'prefijo': 'DTPT', 'pais': 'Colombia', 'tipo_calculo': 'C'},
            '6-COMPRAFACIL': {'prefijo': 'CFA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
            '7-COMPRA-YA': {'prefijo': 'CPYA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
            '8-FABORCARGO': {'prefijo': 'FBC', 'pais': 'Colombia', 'tipo_calculo': 'D'}
        }


@st.cache_data(ttl=600) # Cache por 10 minutos
def get_trm_rates():
    """Obtiene las últimas tasas TRM"""
    try:
        supabase = init_supabase()
        if supabase:
            # Intenta ordenar por 'created_at'. Si no existe o da error, usa 'id'.
            try:
                result = supabase.table('trm_rates').select('*').order('created_at', desc=True).execute()
            except Exception as e:
                # Fallback si 'created_at' no existe o la consulta falla
                st.warning(f"⚠️ No se pudo ordenar por 'created_at': {e}. Intentando ordenar por 'id'. Asegúrate de que 'created_at' sea una columna de tipo timestamp en tu tabla 'trm_rates' de Supabase con DEFAULT now().")
                result = supabase.table('trm_rates').select('*').order('id', desc=True).execute()
                
            if result.data:
                df_trm = pd.DataFrame(result.data)
                # Convertir a datetime si las columnas existen
                for col_date in ['date_updated', 'created_at']:
                    if col_date in df_trm.columns:
                        df_trm[col_date] = pd.to_datetime(df_trm[col_date], errors='coerce', infer_datetime_format=True)
                
                # Asegurarse de que 'id' es numérico
                if 'id' in df_trm.columns:
                    df_trm['id'] = pd.to_numeric(df_trm['id'], errors='coerce')
                    df_trm = df_trm.dropna(subset=['id'])

                # Ordenar por moneda y luego por la columna de fecha más fiable para obtener la última TRM por moneda
                if 'created_at' in df_trm.columns and not df_trm['created_at'].isnull().all():
                    df_trm = df_trm.sort_values(by=['currency', 'created_at'], ascending=[True, False])
                else:
                    df_trm = df_trm.sort_values(by=['currency', 'id'], ascending=[True, False])
                
                df_trm = df_trm.drop_duplicates(subset=['currency'], keep='first')
                return {row['currency']: row['rate'] for _, row in df_trm.iterrows()}
        # Fallback de TRM si Supabase no devuelve datos
        return {'COP': 4000.0, 'PEN': 3.8, 'CLP': 900.0}
    except Exception as e:
        st.error(f"Error obteniendo TRM: {str(e)}. Usando valores por defecto.")
        return {'COP': 4000.0, 'PEN': 3.8, 'CLP': 900.0}


def save_trm_rates(trm_data):
    """Guarda tasas TRM en Supabase"""
    try:
        supabase = init_supabase()
        if supabase:
            records_to_insert = []
            for currency, rate in trm_data.items():
                if currency != 'last_update':
                    records_to_insert.append({
                        'currency': currency,
                        'rate': float(rate),
                        'updated_by': 'streamlit_app',
                        'date_updated': datetime.now().isoformat(), # Asegurarse de que esta columna exista
                        'created_at': datetime.now().isoformat() # Asegurarse de que esta columna exista y sea timestamp con default now() en DB
                    })
            if records_to_insert:
                # Supabase inserta automáticamente 'created_at' si la columna está configurada con DEFAULT now()
                # y no se pasa en el insert. Aquí la estamos pasando explícitamente.
                result = supabase.table('trm_rates').insert(records_to_insert).execute()
                return True, "TRM guardado exitosamente"
            return False, "No hay datos TRM para guardar"
        return False, "No se pudo conectar a Supabase para guardar TRM"
    except Exception as e:
        return False, f"Error al guardar TRM en Supabase: {str(e)}"

def save_orders_to_supabase(df_processed_for_save):
    """Guarda órdenes procesadas en Supabase.
    df_processed_for_save es un DataFrame que ya ha sido agregado por order_id.
    """
    try:
        supabase = init_supabase()
        if not supabase:
            return False, "No hay conexión a Supabase"
        
        orders_data = []
        for _, row in df_processed_for_save.iterrows():
            order_dict = {
                'order_id': str(row.get('order_id', '')),
                'account_name': str(row.get('account_name', '')),
                'serial_number': str(row.get('serial_number', '')), # Ahora se espera 'serial_number' del df agregado
                'asignacion': str(row.get('asignacion', '')), # Ahora se espera 'asignacion' del df agregado
                'pais': str(row.get('pais', '')),
                'tipo_calculo': str(row.get('tipo_calculo', '')),
                'moneda': str(row.get('moneda', '')),
                'date_created': row.get('date_created').isoformat() if pd.notna(row.get('date_created')) and isinstance(row.get('date_created'), datetime) else None,
                'quantity': int(row.get('quantity', 0)),
                'logistic_type': str(row.get('logistic_type', '')),
                'order_status_meli': str(row.get('order_status_meli', '')),
                'declare_value': float(row.get('declare_value', 0)),
                'net_real_amount': float(row.get('net_real_amount', 0)),
                'logistic_weight_lbs': float(row.get('logistic_weight_lbs', 0)),
                'meli_usd': float(row.get('meli_usd', 0)),
                'costo_amazon': float(row.get('costo_amazon', 0)),
                'total_anican': float(row.get('total_anican', 0)),
                'aditional': float(row.get('aditional', 0)),
                'bodegal': float(row.get('bodegal', 0)),
                'socio_cuenta': float(row.get('socio_cuenta', 0)),
                'costo_cxp': float(row.get('costo_cxp', 0)),
                'impuesto_facturacion': float(row.get('impuesto_facturacion', 0)),
                'gss_logistica': float(row.get('gss_logistica', 0)),
                'impuesto_gss': float(row.get('impuesto_gss', 0)),
                'utilidad_gss': float(row.get('utilidad_gss', 0)),
                'utilidad_socio': float(row.get('utilidad_socio', 0))
            }
            orders_data.append(order_dict)
            
        # Insertar en lotes
        batch_size = 100 # Puedes ajustar este tamaño si tienes problemas de rendimiento o límites de Supabase
        total_inserted = 0
        
        for i in range(0, len(orders_data), batch_size):
            batch = orders_data[i:i+batch_size]
            # Usamos upsert con on_conflict='order_id' para actualizar si ya existe, insertar si no.
            # Asegúrate que 'order_id' es la PK en tu tabla 'orders' en Supabase.
            result = supabase.table('orders').upsert(batch, on_conflict='order_id').execute()
            total_inserted += len(batch)
            
        return True, f"{total_inserted} órdenes guardadas/actualizadas"
        
    except Exception as e:
        return False, f"Error al guardar órdenes en Supabase: {str(e)}"

@st.cache_data(ttl=60) # Cache por 1 minuto
def get_orders_from_supabase(limit=1000):
    """Obtiene órdenes desde Supabase"""
    try:
        supabase = init_supabase()
        if supabase:
            try:
                # Intentar ordenar por 'created_at' si existe en la tabla 'orders'
                result = supabase.table('orders').select('*').order('created_at', desc=True).limit(limit).execute()
            except Exception as e:
                # Fallback a 'order_id' si 'created_at' no existe o falla
                st.warning(f"⚠️ No se pudo ordenar por 'created_at' en tabla 'orders': {e}. Intentando ordenar por 'order_id'.")
                result = supabase.table('orders').select('*').order('order_id', desc=True).limit(limit).execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                # Convertir columnas de fecha si existen
                for col_date in ['date_created', 'created_at']:
                    if col_date in df.columns:
                        df[col_date] = pd.to_datetime(df[col_date], errors='coerce', infer_datetime_format=True)
                return True, df
        return False, "No hay datos en la tabla 'orders' de Supabase."
    except Exception as e:
        st.error(f"Error obteniendo órdenes desde Supabase: {str(e)}")
        return False, pd.DataFrame() # Retorna DataFrame vacío en caso de error


# ============================
# FUNCIONES DE NEGOCIO
# ============================

def calcular_asignacion(account_name, serial_number, store_config):
    """Calcula la columna Asignacion"""
    if pd.isna(account_name) or pd.isna(serial_number):
        return None
    
    account_str = str(account_name).strip()
    prefijo = store_config.get(account_str, {}).get('prefijo', '')
    
    if prefijo:
        return f"{prefijo}{serial_number}"
    return None

def obtener_gss_logistica(peso_kg):
    """Obtiene valor de GSS según peso"""
    ANEXO_A = [
        (0.01, 0.50, 24.01), (0.51, 1.00, 33.09), (1.01, 1.50, 42.17), (1.51, 2.00, 51.25),
        (2.01, 2.50, 61.94), (2.51, 3.00, 71.02), (3.01, 3.50, 80.91), (3.51, 4.00, 89.99),
        (4.01, 4.50, 99.87), (4.51, 5.00, 108.95), (5.01, 5.50, 117.19), (5.51, 6.00, 126.12),
        (6.01, 6.50, 135.85), (6.51, 7.00, 144.78), (7.01, 7.50, 154.52), (7.51, 8.00, 163.75),
        (8.01, 8.50, 173.18), (8.51, 9.00, 182.11), (9.01, 9.50, 191.85), (9.51, 10.00, 200.78),
        (10.01, 10.50, 207.36), (10.51, 11.00, 216.14), (11.01, 11.50, 225.73), (11.51, 12.00, 234.51),
        (12.01, 12.50, 244.09), (12.51, 13.00, 252.87), (13.01, 13.50, 262.46), (13.51, 14.00, 271.24),
        (14.01, 14.50, 280.82), (14.51, 15.00, 289.60), (15.01, 15.50, 294.54), (15.51, 16.00, 303.17),
        (16.01, 16.50, 312.60), (16.51, 17.00, 321.23), (17.01, 17.50, 330.67), (17.51, 18.00, 339.30),
        (18.01, 18.50, 348.73), (18.51, 19.00, 357.36), (19.01, 19.50, 366.80), (19.51, 20.00, 373.72)
    ]
    
    if pd.isna(peso_kg) or peso_kg <= 0:
        return 0
    
    for desde, hasta, gss_value in ANEXO_A:
        if desde <= peso_kg <= hasta:
            return gss_value
    
    return ANEXO_A[-1][2] # Último valor si excede el rango


def calcular_utilidades(df, store_config, trm_data):
    """Calcula utilidades según tipo de tienda.
       Asume que df es el DataFrame de Drapify pre-procesado."""
    
    # Asegurar que las columnas de referencia sean strings
    df['Serial#'] = df.get('Serial#', pd.Series(dtype=str)).astype(str)
    df['order_id'] = df.get('order_id', pd.Series(dtype=str)).astype(str)
    
    # Calcular columna Asignacion
    df['Asignacion'] = df.apply(
        lambda row: calcular_asignacion(row['account_name'], row.get('Serial#', ''), store_config),
        axis=1
    )
    
    # Mapear país y tipo de cálculo
    df['pais'] = df['account_name'].map(
        lambda x: store_config.get(str(x), {}).get('pais', 'Desconocido')
    )
    df['tipo_calculo'] = df['account_name'].map(
        lambda x: store_config.get(str(x), {}).get('tipo_calculo', 'A')
    )
    
    # Mapear moneda
    pais_moneda = {'Colombia': 'COP', 'Perú': 'PEN', 'Peru': 'PEN', 'Chile': 'CLP'}
    df['moneda'] = df['pais'].map(pais_moneda)
    
    # Calcular MELI USD
    df['MELI USD'] = df.apply(
        lambda row: (row['net_real_amount'] / trm_data.get(row['moneda'], 1))
        if pd.notna(row['net_real_amount']) and row['moneda'] in trm_data else 0,
        axis=1
    )
    
    # Inicializar columnas que pueden venir de merges o cálculos específicos
    # y asegurar su tipo numérico para operaciones posteriores
    cols_to_init_float = [
        'Aditional', 'Bodegal', 'Socio_cuenta', 'Impuesto por facturacion', 
        'Gss Logistica', 'Impuesto Gss', 'Utilidad Gss', 'Utilidad Socio',
        'Total_Anican', 'Costo cxp', 'Amt_Due_CXP', 'Arancel_CXP', 'IVA_CXP', 'Peso_kg'
    ]
    for col in cols_to_init_float:
        if col not in df.columns: # Solo inicializar si no existen
            df[col] = 0.0
        else:
            # Asegurar que la columna es numérica y rellenar NaN si existen
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # Convertir 'Declare Value' y 'quantity' a numérico al inicio de la función
    # para evitar el error 'float' object has no attribute 'fillna'
    df['Declare Value'] = pd.to_numeric(df['Declare Value'], errors='coerce').fillna(0)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(1) # quantity defaults to 1 if missing for multiplication

    df['Costo Amazon'] = df['Declare Value'] * df['quantity']

    # El resto de los cálculos específicos por tipo se harán en el loop principal
    # después de que todos los archivos (Anican, Aditionals, CXP) hayan sido fusionados.
    # Esta función `calcular_utilidades` solo prepara las columnas comunes.
    return df

# ============================
# INICIALIZAR SESSION STATE
# ============================

# Inicializar `processed_data` una sola vez
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None

# Inicializar TRM y Store Config desde Supabase
# Se cargan una vez y se actualizan si el usuario las cambia
if 'trm_data' not in st.session_state:
    st.session_state.trm_data = get_trm_rates()
    st.session_state.trm_data['last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if 'store_config' not in st.session_state:
    st.session_state.store_config = get_store_config()


# ============================
# INTERFAZ PRINCIPAL
# ============================

# Header
st.markdown("""
<div class="main-header">
    <h1>🌎 Sistema Contable Multi-País</h1>
    <p>Versión Cloud con Supabase Database</p>
</div>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.title("🎛️ Panel de Control")

# Estado de conexión
st.sidebar.markdown("### 🔗 Estado de Conexión")
connected, message = test_connection()
if connected:
    st.sidebar.success(message)
else:
    st.sidebar.error(message)

# Navegación
page = st.sidebar.selectbox("Selecciona una sección:", [
    "🏠 Inicio",
    "📊 Dashboard en Tiempo Real",
    "📁 Procesar Archivos",
    "💱 Configurar TRM",
    "📋 Fórmulas de Negocio"
])

# TRM actual
st.sidebar.markdown("### 💱 TRM Actual")
for currency, rate in st.session_state.trm_data.items():
    if currency != 'last_update':
        st.sidebar.metric(f"{currency}/USD", f"{rate:,.2f}")

# ============================
# PÁGINAS
# ============================

if page == "🏠 Inicio":
    st.header("🏠 Bienvenido al Sistema Contable")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <h3>📊 Dashboard</h3>
            <p>Visualiza utilidades en tiempo real desde Supabase</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <h3>📁 Procesar</h3>
            <p>Carga y procesa archivos Excel automáticamente</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card">
            <h3>🗄️ Datos</h3>
            <p>Gestiona datos almacenados en la nube</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Estadísticas rápidas
    try:
        success, df = get_orders_from_supabase(100)
        
        if success and len(df) > 0:
            st.markdown("### 📈 Resumen Rápido (Últimas 100 órdenes)")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📦 Total Órdenes", len(df))
            with col2:
                total_utilidad = df['utilidad_gss'].sum() + df['utilidad_socio'].sum()
                st.metric("Utilidad Total", f"${total_utilidad:,.2f}")
            with col3:
                paises = df['pais'].nunique()
                st.metric("Países Activos", paises)
            with col4:
                tiendas = df['account_name'].nunique()
                st.metric("Tiendas Activas", tiendas)
        else:
            st.markdown("""
            <div class="info-box">
                <h4>🚀 ¡Comienza aquí!</h4>
                <p>No hay datos en Supabase aún. Ve a <strong>"Procesar Archivos"</strong> para cargar tu primera información.</p>
            </div>
            """, unsafe_allow_html=True)
        
    except Exception as e:
        st.warning(f"⚠️ No se pudo cargar resumen: {str(e)}")


elif page == "📊 Dashboard en Tiempo Real":
    st.header("📊 Dashboard en Tiempo Real")
    
    try:
        success, df = get_orders_from_supabase(500) # Se obtienen 500 órdenes para el dashboard
        
        if success and len(df) > 0:
            # Métricas principales
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📦 Total Órdenes", len(df))
            with col2:
                utilidad_gss = df['utilidad_gss'].sum()
                st.metric("🏢 Utilidad GSS", f"${utilidad_gss:,.2f}")
            with col3:
                utilidad_socio = df['utilidad_socio'].sum()
                st.metric("🤝 Utilidad Socio", f"${utilidad_socio:,.2f}")
            with col4:
                total_ingresos = df['meli_usd'].sum()
                st.metric("💰 Ingresos Total", f"${total_ingresos:,.2f}")
            
            # Gráficos
            st.markdown("### 📊 Análisis Visual")
            
            tab1, tab2 = st.tabs(["Por País", "Por Tienda"])
            
            with tab1:
                # Utilidades por país
                pais_data = df.groupby('pais').agg({
                    'utilidad_gss': 'sum',
                    'utilidad_socio': 'sum',
                    'order_id': 'count'
                }).reset_index()
                
                fig_pais = px.bar(
                    pais_data, 
                    x='pais', 
                    y=['utilidad_gss', 'utilidad_socio'],
                    title='Utilidades por País (USD)',
                    height=400,
                    labels={'value': 'Utilidad (USD)', 'variable': 'Tipo de Utilidad'},
                    color_discrete_map={'utilidad_gss': '#007bff', 'utilidad_socio': '#28a745'}
                )
                st.plotly_chart(fig_pais, use_container_width=True)
            
            with tab2:
                # Top tiendas
                tienda_data = df.groupby('account_name').agg({
                    'utilidad_gss': 'sum',
                    'utilidad_socio': 'sum'
                }).reset_index()
                tienda_data['total'] = tienda_data['utilidad_gss'] + tienda_data['utilidad_socio']
                tienda_data = tienda_data.sort_values('total', ascending=True).tail(10) # Top 10
                
                fig_tienda = px.bar(
                    tienda_data, 
                    x='total', 
                    y='account_name',
                    orientation='h',
                    title='Top 10 Tiendas por Utilidad Total',
                    height=500,
                    labels={'total': 'Utilidad Total (USD)', 'account_name': 'Nombre de Tienda'},
                    color_discrete_sequence=px.colors.sequential.Viridis
                )
                st.plotly_chart(fig_tienda, use_container_width=True)
            
            # Tabla de datos
            st.markdown("### 📋 Datos Recientes")
            st.dataframe(
                df[['order_id', 'account_name', 'pais', 'meli_usd', 'utilidad_gss', 'utilidad_socio']].head(20),
                use_container_width=True
            )
        
        else:
            st.warning("No hay datos disponibles en Supabase para mostrar en el Dashboard. Por favor, procesa algunos archivos primero.")
    
    except Exception as e:
        st.error(f"Error cargando dashboard: {str(e)}")


elif page == "📁 Procesar Archivos":
    st.header("📁 Cargar y Procesar Archivos")
    
    st.markdown("""
    <div class="info-box">
        <h4>🔄 Proceso de Carga</h4>
        <p>Sube los archivos necesarios (DRAPIFY, Anican Logistics, Anican Aditionals, Chile Express CXP) para calcular las utilidades según las fórmulas de negocio.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Crear columnas para organizar las cargas
    col_drapify, col_anican_cxp = st.columns(2)

    with col_drapify:
        st.markdown("### 📊 Archivo Principal: DRAPIFY")
        drapify_file = st.file_uploader(
            "📄 Cargar DRAPIFY (Orders_XXXXXXXX)",
            type=['xlsx', 'xls'],
            key="drapify_uploader", # Clave única para el uploader
            help="Archivo principal con todas las órdenes de MercadoLibre."
        )

    with col_anican_cxp:
        st.markdown("### 🚚 Archivos Logísticos y Adicionales")
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
            help="Archivo de Chile Express con costos logísticos y de aduana (Arancel, IVA)."
        )

    
    if st.button("🚀 Procesar y Guardar en Supabase", type="primary"):
        if drapify_file:
            try:
                with st.spinner("Procesando archivos... Esto puede tardar unos segundos."):
                    # 1. PROCESAR ARCHIVO DRAPIFY
                    df_drapify = pd.read_excel(drapify_file)

                    # Verificar columnas requeridas en DRAPIFY
                    columnas_requeridas_drapify = [
                        'System#', 'Serial#', 'order_id', 'account_name',
                        'date_created', 'quantity', 'logistic_type',
                        'order_status_meli', 'ETIQUETA_ENVIO', 'Declare Value',
                        'net_real_amount', 'logistic_weight_lbs',
                        'refunded_date'
                    ]

                    columnas_faltantes_drapify = [
                        col for col in columnas_requeridas_drapify
                        if col not in df_drapify.columns
                    ]

                    if columnas_faltantes_drapify:
                        st.error(
                            f"❌ Faltan columnas en el archivo DRAPIFY: {', '.join(columnas_faltantes_drapify)}"
                        )
                        st.stop() 
                    
                    # Inicializar el DataFrame base con las columnas de Drapify y las comunes calculadas
                    df_processed = calcular_utilidades(df_drapify.copy(), st.session_state.store_config, st.session_state.trm_data)

                    # 2. PROCESAR ARCHIVO ANICAN LOGISTICS (si existe)
                    if anican_file:
                        try:
                            df_anican = pd.read_excel(anican_file)
                            required_anican_cols = ['Reference', 'Total']
                            
                            if all(col in df_anican.columns for col in required_anican_cols):
                                df_anican['Reference'] = df_anican['Reference'].astype(str)
                                df_processed = df_processed.merge(
                                    df_anican[['Reference', 'Total']].rename(
                                        columns={'Reference': 'order_id', 'Total': 'Total_Anican_Merge'}),
                                    on='order_id',
                                    how='left'
                                )
                                df_processed['Total_Anican'] = df_processed['Total_Anican_Merge'].fillna(0)
                                if 'Total_Anican_Merge' in df_processed.columns:
                                    df_processed = df_processed.drop(columns=['Total_Anican_Merge'])
                                st.success(f"✅ Anican Logistics procesado: {df_processed['Total_Anican'].gt(0).sum()} coincidencias.")
                            else:
                                st.warning("⚠️ Archivo Anican Logistics no tiene las columnas esperadas ('Reference', 'Total').")
                        except Exception as e:
                            st.error(f"❌ Error procesando Anican Logistics: {str(e)}")
                    else:
                        st.info("ℹ️ Archivo Anican Logistics no cargado. Se omitirá su procesamiento.")

                    # 3. PROCESAR ARCHIVO ANICAN ADITIONALS (si existe)
                    if aditionals_file:
                        try:
                            df_aditionals = pd.read_excel(aditionals_file)
                            required_aditionals_cols = ['Order Id', 'Quantity', 'UnitPrice']
                            
                            if all(col in df_aditionals.columns for col in required_aditionals_cols):
                                df_aditionals['Aditional_calc_temp'] = df_aditionals['Quantity'].fillna(0) * df_aditionals['UnitPrice'].fillna(0)
                                aditionals_grouped = df_aditionals.groupby('Order Id')['Aditional_calc_temp'].sum().reset_index()
                                
                                # Renombrar para el merge
                                aditionals_grouped = aditionals_grouped.rename(
                                    columns={'Order Id': 'order_id', 'Aditional_calc_temp': 'Aditional_Merge'})
                                aditionals_grouped['order_id'] = aditionals_grouped['order_id'].astype(str)

                                df_processed = df_processed.merge(
                                    aditionals_grouped[['order_id', 'Aditional_Merge']],
                                    on='order_id',
                                    how='left'
                                )
                                df_processed['Aditional'] = df_processed['Aditional_Merge'].fillna(0)
                                if 'Aditional_Merge' in df_processed.columns:
                                    df_processed = df_processed.drop(columns=['Aditional_Merge'])
                                st.success(f"✅ Anican Aditionals procesado: {df_processed['Aditional'].gt(0).sum()} coincidencias.")
                            else:
                                st.warning("⚠️ Archivo Anican Aditionals no tiene las columnas esperadas ('Order Id', 'Quantity', 'UnitPrice').")
                        except Exception as e:
                            st.error(f"❌ Error procesando Anican Aditionals: {str(e)}")
                    else:
                        st.info("ℹ️ Archivo Anican Aditionals no cargado. Se omitirá su procesamiento.")

                    # 4. PROCESAR ARCHIVO CXP (si existe)
                    if cxp_file:
                        try:
                            st.info("🔧 Iniciando procesamiento de Chile Express (CXP)...")
                            excel_file_cxp = pd.ExcelFile(cxp_file)
                            df_cxp = None
                            
                            for sheet_name in excel_file_cxp.sheet_names:
                                for header_row in range(5): # Probar encabezados en las primeras 5 filas (0-4)
                                    try:
                                        df_test_cxp = pd.read_excel(cxp_file, sheet_name=sheet_name, header=header_row)
                                        df_test_cxp.columns = [str(col).strip() for col in df_test_cxp.columns] # Limpiar nombres de columna
                                        
                                        # Buscar columnas por patrones flexibles
                                        ref_col = next((col for col in df_test_cxp.columns if re.search(r'Ref\s*#', col, re.IGNORECASE)), None)
                                        amt_col = next((col for col in df_test_cxp.columns if re.search(r'Amt\.\s*Due', col, re.IGNORECASE)), None)
                                        arancel_col = next((col for col in df_test_cxp.columns if re.search(r'Arancel', col, re.IGNORECASE)), None)
                                        iva_col = next((col for col in df_test_cxp.columns if re.search(r'IVA', col, re.IGNORECASE)), None)

                                        if ref_col and amt_col:
                                            df_cxp = df_test_cxp[[ref_col, amt_col]].copy()
                                            df_cxp = df_cxp.rename(columns={ref_col: 'Asignacion_cxp', amt_col: 'Amt_Due_CXP'})
                                            
                                            # Asegurar que las columnas del CXP son numéricas antes del merge
                                            df_cxp['Amt_Due_CXP'] = pd.to_numeric(df_cxp['Amt_Due_CXP'], errors='coerce').fillna(0)
                                            if arancel_col: df_cxp['Arancel_CXP_Merge'] = pd.to_numeric(df_test_cxp[arancel_col], errors='coerce').fillna(0)
                                            if iva_col: df_cxp['IVA_CXP_Merge'] = pd.to_numeric(df_test_cxp[iva_col], errors='coerce').fillna(0)
                                            
                                            st.info(f"✅ CXP: Encabezado detectado en hoja '{sheet_name}', fila {header_row}. Columnas: {list(df_cxp.columns)}")
                                            break # Salir de loop de headers si encontramos
                                    except Exception as e:
                                        # No mostrar error para cada intento, es normal que fallen varios.
                                        pass 
                                if df_cxp is not None:
                                    break # Salir de loop de hojas si encontramos
                            
                            if df_cxp is not None:
                                df_cxp['Asignacion_cxp'] = df_cxp['Asignacion_cxp'].astype(str).str.strip()
                                
                                # Unir con el DataFrame principal
                                df_processed = df_processed.merge(
                                    df_cxp[['Asignacion_cxp', 'Amt_Due_CXP', 'Arancel_CXP_Merge', 'IVA_CXP_Merge']].rename(
                                        columns={'Arancel_CXP_Merge': 'Arancel_CXP', 'IVA_CXP_Merge': 'IVA_CXP'}),
                                    left_on='Asignacion',
                                    right_on='Asignacion_cxp',
                                    how='left'
                                )
                                # Fill NaNs for merged columns. These were already handled in df_cxp, but for non-matches they will be NaN.
                                df_processed['Amt_Due_CXP'] = df_processed['Amt_Due_CXP'].fillna(0)
                                df_processed['Arancel_CXP'] = df_processed['Arancel_CXP'].fillna(0)
                                df_processed['IVA_CXP'] = df_processed['IVA_CXP'].fillna(0)
                                df_processed['Costo cxp'] = df_processed['Amt_Due_CXP'] # Según la fórmula
                                df_processed['Impuesto Gss'] = df_processed['Arancel_CXP'] + df_processed['IVA_CXP'] # Según la fórmula

                                # Limpiar columnas temporales del merge
                                if 'Asignacion_cxp' in df_processed.columns:
                                    df_processed = df_processed.drop(columns=['Asignacion_cxp'])

                                st.success(f"✅ Chile Express (CXP) procesado exitosamente. {df_processed['Amt_Due_CXP'].gt(0).sum()} coincidencias.")
                            else:
                                st.warning("⚠️ No se pudo encontrar las columnas 'Ref #' y 'Amt. Due' en el archivo CXP.")
                        except Exception as e:
                            st.error(f"❌ Error procesando Chile Express (CXP): {str(e)}")
                    else:
                        st.info("ℹ️ Archivo Chile Express (CXP) no cargado. Se omitirá su procesamiento.")

                    # --- RE-CALCULAR CAMPOS ESPECÍFICOS Y UTILIDADES DESPUÉS DE MERGES ---
                    # Ahora que todos los datos están fusionados, aplicar los cálculos finales

                    # Asegurar que logistic_weight_lbs y quantity sean numéricos y no NaN
                    df_processed['logistic_weight_lbs'] = pd.to_numeric(df_processed['logistic_weight_lbs'], errors='coerce').fillna(0)
                    df_processed['quantity'] = pd.to_numeric(df_processed['quantity'], errors='coerce').fillna(1) # quantity defaults to 1 if missing for multiplication

                    df_processed['Bodegal'] = df_processed['logistic_type'].apply(lambda x: 3.5 if str(x).lower() == 'xd_drop_off' else 0)
                    df_processed['Socio_cuenta'] = df_processed['order_status_meli'].apply(lambda x: 0 if str(x).lower() == 'refunded' else 1)
                    df_processed['Impuesto por facturacion'] = df_processed['order_status_meli'].apply(lambda x: 1 if str(x).lower() in ['approved', 'in mediation'] else 0)
                    
                    df_processed['Peso_kg'] = (df_processed['logistic_weight_lbs'] * df_processed['quantity']) * 0.453592
                    df_processed['Gss Logistica'] = df_processed['Peso_kg'].apply(obtener_gss_logistica)

                    # Final calcular Utilidad Gss y Utilidad Socio (separamos la lógica por tipo de cálculo)
                    def apply_final_profit_calculation(row):
                        tipo = row.get('tipo_calculo', 'A') # Asegurar que tipo_calculo también se acceda con .get()
                        # Usar .get() para acceder a las columnas y proporcionar un valor por defecto de 0.0
                        # Esto previene KeyErrors si por alguna razón una columna no está presente
                        meli_usd = row.get('MELI USD', 0.0)
                        costo_amazon = row.get('Costo Amazon', 0.0)
                        total_anican = row.get('Total_Anican', 0.0)
                        aditional = row.get('Aditional', 0.0)
                        costo_cxp = row.get('Costo cxp', 0.0)
                        bodegal = row.get('Bodegal', 0.0)
                        socio_cuenta = row.get('Socio_cuenta', 0.0)
                        impuesto_facturacion = row.get('Impuesto por facturacion', 0.0)
                        gss_logistica = row.get('Gss Logistica', 0.0)
                        impuesto_gss = row.get('Impuesto Gss', 0.0)
                        amt_due_cxp = row.get('Amt_Due_CXP', 0.0) # Equivalente a Costo cxp para D

                        utilidad_gss_final = 0.0
                        utilidad_socio_final = 0.0

                        if tipo == 'A': # TODOENCARGO-CO, MEGA TIENDAS PERUANAS
                            utilidad_gss_final = meli_usd - costo_amazon - total_anican - aditional
                        elif tipo == 'B': # MEGATIENDA SPA, VEENDELO
                            utilidad_gss_final = meli_usd - costo_cxp - costo_amazon - bodegal - socio_cuenta
                        elif tipo == 'C': # DETODOPARATODOS, COMPRAFACIL, COMPRA-YA
                            utilidad_base_c = meli_usd - costo_amazon - total_anican - aditional - impuesto_facturacion
                            if utilidad_base_c > 7.5:
                                utilidad_socio_final = 7.5
                                utilidad_gss_final = utilidad_base_c - utilidad_socio_final
                            else:
                                utilidad_socio_final = utilidad_base_c
                                utilidad_gss_final = 0
                        elif tipo == 'D': # FABORCARGO
                            utilidad_gss_final = gss_logistica + impuesto_gss - amt_due_cxp
                        
                        return pd.Series([utilidad_gss_final, utilidad_socio_final], index=['Utilidad Gss', 'Utilidad Socio'])

                    df_processed[['Utilidad Gss', 'Utilidad Socio']] = df_processed.apply(apply_final_profit_calculation, axis=1)


                    st.session_state.processed_data = df_processed
                    st.success("✅ Archivos procesados y utilidades calculadas con éxito!")

                    # --- PREPARAR DATOS PARA GUARDAR EN SUPABASE: AGREGAR POR ORDER_ID ---
                    # Esto es CRUCIAL para evitar el error "ON CONFLICT DO UPDATE command cannot affect row a second time"
                    # si hay múltiples líneas de pedido para el mismo order_id en el DataFrame.

                    numeric_cols = [
                        'quantity', 'declare_value', 'net_real_amount', 
                        'logistic_weight_lbs', 'meli_usd', 'costo_amazon', 
                        'total_anican', 'aditional', 'bodegal', 'socio_cuenta', 
                        'costo_cxp', 'impuesto_facturacion', 'gss_logistica', 
                        'impuesto_gss', 'utilidad_gss', 'utilidad_socio'
                    ]
                    # Definir las columnas a tomar la primera aparición (para campos no sumables)
                    first_cols = [
                        'account_name', 'serial_number', 'asignacion', 'pais', 
                        'tipo_calculo', 'moneda', 'logistic_type', 'order_status_meli',
                        'date_created' # date_created debería ser el de la primera aparición
                    ]

                    # Crear un diccionario de funciones de agregación
                    agg_dict = {col: 'sum' for col in numeric_cols if col in df_processed.columns}
                    agg_dict.update({col: 'first' for col in first_cols if col in df_processed.columns})
                    
                    # Realizar la agregación
                    df_to_save_to_supabase = df_processed.groupby('order_id').agg(agg_dict).reset_index()

                    # Guardar en Supabase el DataFrame agregado
                    save_success, save_message = save_orders_to_supabase(df_to_save_to_supabase)
                    if save_success:
                        st.success(f"💾 {save_message}")
                    else:
                        st.error(f"❌ Error al guardar en Supabase: {save_message}")
                    
                    # Limpiar cache para forzar la recarga del dashboard
                    st.cache_data.clear()
                    
                    # Mostrar resumen de lo procesado
                    st.markdown("### 📊 Resumen de Procesamiento")
                    col_p1, col_p2, col_p3, col_p4 = st.columns(4)
                    with col_p1:
                        st.metric("Total Órdenes Procesadas", len(df_processed))
                    with col_p2:
                        st.metric("Utilidad GSS Calculada", f"${df_processed['Utilidad Gss'].sum():,.2f}")
                    with col_p3:
                        st.metric("Utilidad Socio Calculada", f"${df_processed['Utilidad Socio'].sum():,.2f}")
                    with col_p4:
                        st.metric("Tiendas Procesadas", df_processed['account_name'].nunique())
                    
                    st.markdown("### 👀 Vista Previa de Datos Procesados (Primeras 10 filas)")
                    st.dataframe(df_processed[['order_id', 'account_name', 'pais', 'MELI USD', 'Costo Amazon', 
                                                'Total_Anican', 'Aditional', 'Costo cxp', 'Impuesto Gss',
                                                'Gss Logistica', 'Utilidad Gss', 'Utilidad Socio']].head(10))

                    # Opción para descargar el DataFrame procesado
                    csv = df_processed.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Descargar datos procesados (CSV)",
                        data=csv,
                        file_name=f"datos_contables_procesados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                    )
            
            except Exception as e:
                st.error(f"❌ Ocurrió un error general durante el procesamiento: {str(e)}")
                st.exception(e)
        else:
            st.warning("⚠️ Por favor, carga el archivo 'DRAPIFY (Orders_XXXXXXXX)' para iniciar el procesamiento.")


elif page == "💱 Configurar TRM":
    st.header("💱 Configuración de Tasas de Cambio")
    
    # Recargar TRM actual para mostrar siempre lo último antes de la edición
    st.session_state.trm_data = get_trm_rates()

    with st.form("trm_form"):
        st.markdown("### Tasas de Cambio Actuales (USD a Moneda Local)")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("### 🇨🇴 Colombia")
            cop_trm = st.number_input(
                "COP por 1 USD",
                value=float(st.session_state.trm_data.get('COP', 4000.0)),
                step=50.0,
                min_value=1000.0,
                max_value=10000.0,
                format="%.2f"
            )
        
        with col2:
            st.markdown("### 🇵🇪 Perú")
            pen_trm = st.number_input(
                "PEN por 1 USD",
                value=float(st.session_state.trm_data.get('PEN', 3.8)),
                step=0.01, # Ajustado para precisión de PEN
                min_value=1.0,
                max_value=10.0,
                format="%.3f" # Ajustado para precisión de PEN
            )
        
        with col3:
            st.markdown("### 🇨🇱 Chile")
            clp_trm = st.number_input(
                "CLP por 1 USD",
                value=float(st.session_state.trm_data.get('CLP', 900.0)),
                step=10.0,
                min_value=500.0,
                max_value=1500.0,
                format="%.2f"
            )
        
        submitted = st.form_submit_button("💾 Actualizar TRM", type="primary")
    
    if submitted:
        try:
            new_trm_data = {
                'COP': cop_trm,
                'PEN': pen_trm,
                'CLP': clp_trm,
            }
            
            success, message = save_trm_rates(new_trm_data)
            
            if success:
                st.success("✅ Tasas TRM actualizadas exitosamente y guardadas en Supabase!")
                # Forzar recarga del estado y UI para reflejar los cambios
                st.session_state.trm_data = get_trm_rates() 
                st.session_state.trm_data['last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.rerun() # Recarga la página para que el sidebar también se actualice
            else:
                st.error(f"❌ Error al guardar TRM en Supabase: {message}")
            
        except Exception as e:
            st.error(f"❌ Error actualizando TRM: {str(e)}")


elif page == "📋 Fórmulas de Negocio":
    st.header("📋 Fórmulas de Negocio por Tipo de Tienda")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Tipo A", "Tipo B", "Tipo C", "Tipo D"])
    
    with tab1:
        st.markdown("### Tipo A: TODOENCARGO-CO y MEGA TIENDAS PERUANAS")
        st.markdown("""
        <div class="formula-box">
        <h4>📐 Fórmula Principal</h4>
        <strong>Utilidad Gss = MELI USD - Costo Amazon - Total - Aditional</strong><br><br>
        
        <h5>🔧 Componentes:</h5>
        • <strong>Costo Amazon:</strong> Declare Value × quantity<br>
        • <strong>Aditional:</strong> Quantity × UnitPrice (del archivo Aditionals)<br>
        • <strong>MELI USD:</strong> net_real_amount / TRM<br>
        • <strong>Total:</strong> del archivo Anican Logistics<br><br>
        
        <h5>🌍 Países aplicables:</h5>
        • Colombia (TODOENCARGO-CO)<br>
        • Perú (MEGA TIENDAS PERUANAS)
        </div>
        """, unsafe_allow_html=True)
    
    with tab2:
        st.markdown("### Tipo B: MEGATIENDA SPA y VEENDELO")
        st.markdown("""
        <div class="formula-box">
        <h4>📐 Fórmula Principal</h4>
        <strong>Utilidad Gss = MELI USD - Costo cxp - Costo Amazon - Bodegal - Socio_cuenta</strong><br><br>
        
        <h5>🔧 Componentes:</h5>
        • <strong>Costo cxp:</strong> Amt. Due (del archivo Chile Express CXP)<br>
        • <strong>Bodegal:</strong> 3.5 si logistic_type = "xd_drop_off", sino 0<br>
        • <strong>Socio_cuenta:</strong> 0 si order_status_meli = "refunded", sino 1<        <p>🌎 Gestión financiera unificada para Colombia, Perú y Chile</p>
</div>
""", unsafe_allow_html=True)
