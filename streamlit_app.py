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
        key = st.secrets.get("supabase", {}).get("key", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0") # Corrected key (example, replace with actual)
        
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
        # Fallback configuration (corrected Faborcargo's country)
        return {
            '1-TODOENCARGO-CO': {'prefijo': 'TDC', 'pais': 'Colombia', 'tipo_calculo': 'A'},
            '2-MEGATIENDA SPA': {'prefijo': 'MEGA', 'pais': 'Chile', 'tipo_calculo': 'B'},
            '3-VEENDELO': {'prefijo': 'VEEN', 'pais': 'Colombia', 'tipo_calculo': 'B'},
            '4-MEGA TIENDAS PERUANAS': {'prefijo': 'MGA-PE', 'pais': 'Perú', 'tipo_calculo': 'A'},
            '5-DETODOPARATODOS': {'prefijo': 'DTPT', 'pais': 'Colombia', 'tipo_calculo': 'C'},
            '6-COMPRAFACIL': {'prefijo': 'CFA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
            '7-COMPRA-YA': {'prefijo': 'CPYA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
            '8-FABORCARGO': {'prefijo': 'FBC', 'pais': 'Chile', 'tipo_calculo': 'D'} # Corrected country
        }
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
            '8-FABORCARGO': {'prefijo': 'FBC', 'pais': 'Chile', 'tipo_calculo': 'D'} # Corrected country
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
                st.info(f"ℹ️ No se pudo ordenar por 'created_at' en tabla 'trm_rates': {e}. Intentando ordenar por 'id'. Asegúrate de que 'created_at' sea una columna de tipo timestamp en tu tabla 'trm_rates' de Supabase con DEFAULT now().")
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
                        'date_updated': datetime.now().isoformat(), 
                        # 'created_at': datetime.now().isoformat() # REMOVED: Let Supabase handle if it's DEFAULT now()
                    })
            if records_to_insert:
                result = supabase.table('trm_rates').insert(records_to_insert).execute()
                return True, "TRM guardado exitosamente"
            return False, "No hay datos TRM para guardar"
        return False, f"No se pudo conectar a Supabase para guardar TRM: {str(e)}"
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
                'serial_number': str(row.get('serial', '')), # Usar 'serial' ya que el DataFrame está en snake_case
                'asignacion': str(row.get('asignacion', '')),
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
                # Add other columns from 'first_cols' and 'numeric_cols' here if they are relevant for the DB schema
                # Ensure DB column names match these snake_case keys exactly.
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
                st.info(f"ℹ️ No se pudo ordenar por 'created_at' en tabla 'orders': {e}. Intentando ordenar por 'order_id'.")
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

# Función auxiliar para normalizar nombres de columnas a snake_case
def to_snake_case(name):
    name = str(name) # Ensure name is a string
    name = re.sub(r'[^a-zA-Z0-9_]', '', name) # Eliminar caracteres no deseados
    name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    name = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', name)
    return name.lower().replace(' ', '_').replace('.', '').replace('#', '').replace('-', '_')


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
        (4.01, 4.50, 99.87), (4.51, 5.00, 99.87), (5.01, 5.50, 108.95), (5.51, 6.00, 117.19), # Added value for 4.51 to 5.00
        (6.01, 6.50, 126.12), (6.51, 7.00, 135.85), (7.01, 7.50, 144.78), (7.51, 8.00, 154.52),
        (8.01, 8.50, 163.75), (8.51, 9.00, 182.11), (9.01, 9.50, 191.85), (9.51, 10.00, 191.85), # Corrected 9.51 to 10.00
        (10.01, 10.50, 200.78), (10.51, 11.00, 207.36), (11.01, 11.50, 216.14), (11.51, 12.00, 225.73),
        (12.01, 12.50, 234.51), (12.51, 13.00, 244.09), (13.01, 13.50, 252.87), (13.51, 14.00, 262.46),
        (14.01, 14.50, 271.24), (14.51, 15.00, 280.82), (15.01, 15.50, 289.60), (15.51, 16.00, 294.54),
        (16.01, 16.50, 303.17), (16.51, 17.00, 312.60), (17.01, 17.50, 321.23), (17.51, 18.00, 330.67),
        (18.01, 18.50, 339.30), (18.51, 19.00, 348.73), (19.01, 19.50, 357.36), (19.51, 20.00, 366.80),
        (20.01, float('inf'), 373.72) # Added a catch-all for >20kg
    ]
    
    if pd.isna(peso_kg) or peso_kg <= 0:
        return 0
    
    for desde, hasta, gss_value in ANEXO_A:
        if desde <= peso_kg <= hasta:
            return gss_value
    
    return 0 # Fallback in case of unexpected peso_kg


def calcular_utilidades(df, store_config, trm_data):
    """Calcula utilidades según tipo de tienda.
       Asume que df es el DataFrame de Drapify pre-procesado."""
    
    # Normalizar nombres de columnas del DF principal a snake_case
    df.columns = [to_snake_case(col) for col in df.columns]

    # Asegurar que las columnas de referencia sean strings y existan
    df['serial'] = df.get('serial', pd.Series(dtype=str)).astype(str)
    df['order_id'] = df.get('order_id', pd.Series(dtype=str)).astype(str)
    df['account_name'] = df.get('account_name', pd.Series(dtype=str)).astype(str)
    
    # Calcular columna asignacion
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
    
    # Calcular meli_usd
    df['meli_usd'] = pd.to_numeric(df.get('net_real_amount', 0.0), errors='coerce').fillna(0.0) / df.apply(
        lambda row: trm_data.get(row.get('moneda', ''), 1.0)
        if pd.notna(row.get('net_real_amount')) and row.get('moneda') in trm_data else 1.0, # Default TRM to 1.0 to avoid division by zero
        axis=1
    )
    
    # Inicializar columnas que pueden venir de merges o cálculos específicos
    # y asegurar su tipo numérico para operaciones posteriores
    cols_to_init_float = [
        'aditional', 'bodegal', 'socio_cuenta', 'impuesto_facturacion', 
        'gss_logistica', 'impuesto_gss', 'utilidad_gss', 'utilidad_socio',
        'total_anican', 'costo_cxp', 'amt_due_cxp', 'arancel_cxp', 'iva_cxp', 'peso_kg'
    ]
    for col in cols_to_init_float:
        if col not in df.columns: # Solo inicializar si no existen
            df[col] = 0.0
        else:
            # Asegurar que la columna es numérica y rellenar NaN si existen
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # Convertir 'declare_value' y 'quantity' a numérico al inicio de la función
    df['declare_value'] = pd.to_numeric(df.get('declare_value', 0), errors='coerce').fillna(0.0)
    df['quantity'] = pd.to_numeric(df.get('quantity', 1), errors='coerce').fillna(1.0) # quantity defaults to 1 if missing for multiplication

    df['costo_amazon'] = df['declare_value'] * df['quantity']

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
            type=['xlsx', 'xls', 'csv'], # Added 'csv' type
            key="drapify_uploader", # Clave única para el uploader
            help="Archivo principal con todas las órdenes de MercadoLibre."
        )

    with col_anican_cxp:
        st.markdown("### 🚚 Archivos Logísticos y Adicionales")
        anican_file = st.file_uploader(
            "🚚 Cargar Anican Logistics",
            type=['xlsx', 'xls', 'csv'], # Added 'csv' type
            key="anican_uploader",
            help="Archivo con costos logísticos de Anican."
        )
        aditionals_file = st.file_uploader(
            "➕ Cargar Anican Aditionals",
            type=['xlsx', 'xls', 'csv'], # Added 'csv' type
            key="aditionals_uploader",
            help="Archivo con costos adicionales de Anican."
        )
        cxp_file = st.file_uploader(
            "🇨🇱 Cargar Chile Express (CXP)",
            type=['xlsx', 'xls', 'csv'], # Added 'csv' type
            key="cxp_uploader",
            help="Archivo de Chile Express con costos logísticos y de aduana (Arancel, IVA)."
        )

    
    if st.button("🚀 Procesar y Guardar en Supabase", type="primary"):
        if drapify_file:
            try:
                with st.spinner("Procesando archivos... Esto puede tardar unos segundos."):
                    # 1. PROCESAR ARCHIVO DRAPIFY
                    if drapify_file.name.endswith('.csv'):
                        df_drapify = pd.read_csv(drapify_file)
                    else:
                        df_drapify = pd.read_excel(drapify_file)

                    st.write("DEBUG: df_drapify.head() antes de calcular_utilidades:")
                    st.dataframe(df_drapify.head())
                    st.write("DEBUG: df_drapify.dtypes antes de calcular_utilidades:")
                    st.write(df_drapify.dtypes)


                    # Verificar columnas requeridas en DRAPIFY (usando nombres originales, luego se normalizan)
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
                    # Los nombres de columnas se normalizan dentro de calcular_utilidades
                    df_processed = calcular_utilidades(df_drapify.copy(), st.session_state.store_config, st.session_state.trm_data)

                    st.write("DEBUG: Columnas de df_processed después de calcular_utilidades:", df_processed.columns.tolist())
                    st.write("DEBUG: df_processed.head() después de calcular_utilidades:")
                    st.dataframe(df_processed.head())
                    st.write("DEBUG: df_processed.dtypes después de calcular_utilidades:")
                    st.write(df_processed.dtypes)
                    st.write("DEBUG: Ejemplos de order_id en DRAPIFY (df_processed):", df_processed['order_id'].head().tolist())


                    # 2. PROCESAR ARCHIVO ANICAN LOGISTICS (si existe)
                    if anican_file:
                        try:
                            if anican_file.name.endswith('.csv'):
                                df_anican = pd.read_csv(anican_file)
                            else:
                                df_anican = pd.read_excel(anican_file)

                            df_anican.columns = [to_snake_case(col) for col in df_anican.columns] # Normalizar nombres
                            required_anican_cols = ['reference', 'total'] # Ahora snake_case
                            
                            st.write("DEBUG: Columnas de df_anican después de normalizar:", df_anican.columns.tolist())
                            st.write("DEBUG: df_anican.head() después de normalizar:")
                            st.dataframe(df_anican.head())
                            st.write("DEBUG: Ejemplos de reference en Anican Logistics:", df_anican['reference'].head().tolist())

                            if all(col in df_anican.columns for col in required_anican_cols):
                                df_anican['reference'] = df_anican['reference'].astype(str)
                                
                                # Merge y manejo de columnas
                                # Create a temporary DataFrame with just order_id for merging to avoid modifying df_processed directly
                                # then update df_processed columns based on the merge result.
                                temp_anican_merge_df = df_processed[['order_id']].merge(
                                    df_anican[['reference', 'total']].rename(
                                        columns={'reference': 'order_id', 'total': 'total_anican_merged_temp'}),
                                    on='order_id',
                                    how='left'
                                )
                                # Update the 'total_anican' column in df_processed, filling NaNs with existing values
                                df_processed['total_anican'] = temp_anican_merge_df['total_anican_merged_temp'].fillna(df_processed['total_anican'])
                                st.success(f"✅ Anican Logistics procesado: {df_processed['total_anican'].gt(0).sum()} coincidencias.")
                            else:
                                st.warning("⚠️ Archivo Anican Logistics no tiene las columnas esperadas ('Reference', 'Total').")
                        except Exception as e:
                            st.error(f"❌ Error procesando Anican Logistics: {str(e)}")
                            st.exception(e)
                    else:
                        st.info("ℹ️ Archivo Anican Logistics no cargado. Se omitirá su procesamiento.")

                    st.write("DEBUG: Columnas de df_processed después de Anican Logistics:", df_processed.columns.tolist())

                    # 3. PROCESAR ARCHIVO ANICAN ADITIONALS (si existe)
                    if aditionals_file:
                        try:
                            if aditionals_file.name.endswith('.csv'):
                                df_aditionals = pd.read_csv(aditionals_file)
                            else:
                                df_aditionals = pd.read_excel(aditionals_file)

                            df_aditionals.columns = [to_snake_case(col) for col in df_aditionals.columns] # Normalizar nombres
                            required_aditionals_cols = ['order_id', 'quantity', 'unit_price'] # Ahora snake_case
                            
                            st.write("DEBUG: Columnas de df_aditionals después de normalizar:", df_aditionals.columns.tolist())
                            st.write("DEBUG: df_aditionals.head() después de normalizar:")
                            st.dataframe(df_aditionals.head())
                            st.write("DEBUG: Ejemplos de order_id en Anican Aditionals:", df_aditionals['order_id'].head().tolist())

                            if all(col in df_aditionals.columns for col in required_aditionals_cols):
                                df_aditionals['aditional_calc_temp'] = df_aditionals['quantity'].fillna(0) * df_aditionals['unit_price'].fillna(0)
                                aditionals_grouped = df_aditionals.groupby('order_id')['aditional_calc_temp'].sum().reset_index()
                                
                                # Renombrar para el merge
                                aditionals_grouped = aditionals_grouped.rename(
                                    columns={'aditional_calc_temp': 'aditional_merged_temp'}) # snake_case
                                aditionals_grouped['order_id'] = aditionals_grouped['order_id'].astype(str)

                                # Merge y manejo de columnas
                                temp_aditionals_merge_df = df_processed[['order_id']].merge(
                                    aditionals_grouped[['order_id', 'aditional_merged_temp']],
                                    on='order_id',
                                    how='left'
                                )
                                df_processed['aditional'] = temp_aditionals_merge_df['aditional_merged_temp'].fillna(df_processed['aditional'])
                                st.success(f"✅ Anican Aditionals procesado: {df_processed['aditional'].gt(0).sum()} coincidencias.")
                            else:
                                st.warning("⚠️ Archivo Anican Aditionals no tiene las columnas esperadas ('Order Id', 'Quantity', 'UnitPrice').")
                        except Exception as e:
                            st.error(f"❌ Error procesando Anican Aditionals: {str(e)}")
                            st.exception(e)
                    else:
                        st.info("ℹ️ Archivo Anican Aditionals no cargado. Se omitirá su procesamiento.")
                    
                    st.write("DEBUG: Columnas de df_processed después de Anican Aditionals:", df_processed.columns.tolist())

                    # 4. PROCESAR ARCHIVO CXP (si existe)
                    if cxp_file:
                        try:
                            st.info("🔧 Iniciando procesamiento de Chile Express (CXP)...")
                            df_cxp_raw = None
                            
                            if cxp_file.name.endswith('.csv'):
                                df_cxp_raw = pd.read_csv(cxp_file)
                                # For CSV, assume header is row 0, and then normalize
                                df_cxp_raw.columns = [to_snake_case(col) for col in df_cxp_raw.columns]
                                st.info(f"✅ CXP (CSV): Archivo leído correctamente.")
                            else: # It's an Excel file
                                excel_file_cxp = pd.ExcelFile(cxp_file)
                                for sheet_name in excel_file_cxp.sheet_names:
                                    for header_row in range(5): # Probar encabezados en las primeras 5 filas (0-4)
                                        try:
                                            df_test_cxp = pd.read_excel(cxp_file, sheet_name=sheet_name, header=header_row)
                                            df_test_cxp.columns = [to_snake_case(col) for col in df_test_cxp.columns] # Normalizar nombres
                                            
                                            # Buscar columnas por patrones flexibles (ahora ya son snake_case)
                                            ref_col = next((col for col in df_test_cxp.columns if 'ref' in col), None)
                                            amt_col = next((col for col in df_test_cxp.columns if 'amt_due' in col), None)
                                            
                                            if ref_col and amt_col:
                                                df_cxp_raw = df_test_cxp.copy() # Found the right one
                                                st.info(f"✅ CXP (Excel): Encabezado detectado en hoja '{sheet_name}', fila {header_row}.")
                                                break 
                                        except Exception as e:
                                            pass # Continue trying other headers/sheets
                                    if df_cxp_raw is not None:
                                        break # Found in this sheet, no need to check others

                                if df_cxp_raw is None:
                                    st.warning("⚠️ No se pudo encontrar las columnas 'Ref #' y 'Amt. Due' en ninguna hoja o fila del archivo CXP. Por favor, verifica el formato.")
                                    df_cxp_raw = pd.DataFrame() # Fallback to empty df_cxp to prevent errors later

                            if not df_cxp_raw.empty:
                                # Ensure columns are normalized even if read as CSV directly
                                df_cxp_raw.columns = [to_snake_case(col) for col in df_cxp_raw.columns] 
                                st.write("DEBUG: Columnas de df_cxp_raw después de normalizar:", df_cxp_raw.columns.tolist())
                                st.write("DEBUG: df_cxp_raw.head() después de normalizar:")
                                st.dataframe(df_cxp_raw.head())
                                st.write("DEBUG: Tipos de datos de df_cxp_raw después de normalizar:")
                                st.write(df_cxp_raw.dtypes)

                                # Re-identify columns after normalization
                                ref_col = next((col for col in df_cxp_raw.columns if 'ref' in col), None)
                                amt_col = next((col for col in df_cxp_raw.columns if 'amt_due' in col), None)
                                arancel_col = next((col for col in df_cxp_raw.columns if 'arancel' in col), None)
                                iva_col = next((col for col in df_cxp_raw.columns if 'iva' in col), None)

                                if ref_col and amt_col:
                                    df_cxp_temp = df_cxp_raw[[ref_col, amt_col]].copy()
                                    df_cxp_temp = df_cxp_temp.rename(columns={ref_col: 'asignacion_cxp', amt_col: 'amt_due_cxp_temp'})
                                    
                                    df_cxp_temp['amt_due_cxp_temp'] = pd.to_numeric(df_cxp_temp['amt_due_cxp_temp'], errors='coerce').fillna(0)
                                    if arancel_col: df_cxp_temp['arancel_cxp_merged_temp'] = pd.to_numeric(df_cxp_raw[arancel_col], errors='coerce').fillna(0)
                                    if iva_col: df_cxp_temp['iva_cxp_merged_temp'] = pd.to_numeric(df_cxp_raw[iva_col], errors='coerce').fillna(0)
                                    
                                    df_cxp_temp['asignacion_cxp'] = df_cxp_temp['asignacion_cxp'].astype(str).str.strip()
                                    st.write("DEBUG: Ejemplos de asignacion_cxp en CXP (pre-merge):", df_cxp_temp['asignacion_cxp'].head().tolist())

                                    # Unir con el DataFrame principal, forzar copia
                                    merged_cxp_df = df_processed[['asignacion']].merge( # Merge only on 'asignacion' from df_processed
                                        df_cxp_temp[['asignacion_cxp', 'amt_due_cxp_temp', 'arancel_cxp_merged_temp', 'iva_cxp_merged_temp']].rename(
                                            columns={'arancel_cxp_merged_temp': 'arancel_cxp_temp', 'iva_cxp_merged_temp': 'iva_cxp_temp'}),
                                        left_on='asignacion',
                                        right_on='asignacion_cxp',
                                        how='left'
                                    ).copy() # Ensure it's a copy

                                    # Update df_processed columns with merged values, filling from original if no match
                                    df_processed['amt_due_cxp'] = merged_cxp_df['amt_due_cxp_temp'].fillna(df_processed['amt_due_cxp'])
                                    df_processed['arancel_cxp'] = merged_cxp_df['arancel_cxp_temp'].fillna(df_processed['arancel_cxp'])
                                    df_processed['iva_cxp'] = merged_cxp_df['iva_cxp_temp'].fillna(df_processed['iva_cxp'])

                                    df_processed['costo_cxp'] = df_processed['amt_due_cxp']
                                    df_processed['impuesto_gss'] = df_processed['arancel_cxp'] + df_processed['iva_cxp']

                                    st.success(f"✅ Chile Express (CXP) procesado exitosamente. {df_processed['amt_due_cxp'].gt(0).sum()} coincidencias.")
                                else:
                                    st.warning("⚠️ Archivo CXP no tiene las columnas esperadas ('Ref #', 'Amt. Due') después de la normalización.")
                            else:
                                st.warning("⚠️ Archivo Chile Express (CXP) no pudo ser leído o no contiene los encabezados esperados.")
                        except Exception as e:
                            st.error(f"❌ Error procesando Chile Express (CXP): {str(e)}")
                            st.exception(e)
                    else:
                        st.info("ℹ️ Archivo Chile Express (CXP) no cargado. Se omitirá su procesamiento.")

                    st.write("DEBUG: Columnas de df_processed después de CXP:", df_processed.columns.tolist())
                    st.write("DEBUG: df_processed.head() después de CXP:")
                    st.dataframe(df_processed.head())
                    st.write("DEBUG: Tipos de datos de df_processed después de CXP:")
                    st.write(df_processed.dtypes)


                    # --- APLICAR CÁLCULOS CONDICIONALES DESPUÉS DE TODOS LOS MERGES ---
                    # Esto asegura que todas las columnas base estén disponibles.

                    # Asegurar que logistic_weight_lbs y quantity sean numéricos y no NaN
                    df_processed['logistic_weight_lbs'] = pd.to_numeric(df_processed.get('logistic_weight_lbs', 0), errors='coerce').fillna(0.0)
                    df_processed['quantity'] = pd.to_numeric(df_processed.get('quantity', 1), errors='coerce').fillna(1.0) # quantity defaults to 1 if missing for multiplication

                    # Calcular Bodegal SOLO para Chile
                    bodegal_base = df_processed['logistic_type'].apply(lambda x: 3.5 if str(x).lower() == 'xd_drop_off' else 0.0)
                    df_processed['bodegal'] = bodegal_base * (df_processed['pais'] == 'Chile').astype(float)


                    # Calcular Socio_cuenta SOLO para MEGATIENDA SPA y VEENDELO (Tipo B)
                    socio_cuenta_base = df_processed['order_status_meli'].apply(lambda x: 0.0 if str(x).lower() == 'refunded' else 1.0)
                    df_processed['socio_cuenta'] = socio_cuenta_base * (df_processed['tipo_calculo'] == 'B').astype(float)
                    

                    # Calcular Impuesto por facturacion SOLO para Tipo C
                    impuesto_facturacion_base = df_processed['order_status_meli'].apply(lambda x: 1.0 if str(x).lower() in ['approved', 'in mediation'] else 0.0)
                    df_processed['impuesto_facturacion'] = impuesto_facturacion_base * (df_processed['tipo_calculo'] == 'C').astype(float)

                    # Calcular Gss Logistica y Impuesto Gss SOLO para FABORCARGO (Tipo D)
                    df_processed['peso_kg'] = (df_processed['logistic_weight_lbs'] * df_processed['quantity']) * 0.453592
                    gss_logistica_base = df_processed['peso_kg'].apply(obtener_gss_logistica)
                    
                    df_processed['gss_logistica'] = gss_logistica_base * (df_processed['account_name'] == '8-faborcargo').astype(float) # Corrected to snake_case
                    df_processed['impuesto_gss'] = df_processed['impuesto_gss'] * (df_processed['account_name'] == '8-faborcargo').astype(float) # Corrected to snake_case


                    # Final calcular Utilidad Gss y Utilidad Socio
                    def apply_final_profit_calculation(row):
                        tipo = row.get('tipo_calculo', 'A') 
                        
                        meli_usd = row.get('meli_usd', 0.0)
                        costo_amazon = row.get('costo_amazon', 0.0)
                        total_anican = row.get('total_anican', 0.0)
                        aditional = row.get('aditional', 0.0)
                        costo_cxp = row.get('costo_cxp', 0.0)
                        bodegal = row.get('bodegal', 0.0) # Ya será 0 si no aplica
                        socio_cuenta = row.get('socio_cuenta', 0.0) # Ya será 0 si no aplica
                        impuesto_facturacion = row.get('impuesto_facturacion', 0.0) # Ya será 0 si no aplica
                        gss_logistica = row.get('gss_logistica', 0.0) # Ya será 0 si no aplica
                        impuesto_gss = row.get('impuesto_gss', 0.0) # Ya será 0 si no aplica
                        amt_due_cxp = row.get('amt_due_cxp', 0.0) 

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
                                utilidad_gss_final = utilidad_base_c - 7.5 # Utilidad - Utilidad Socio
                            else:
                                utilidad_socio_final = utilidad_base_c
                                utilidad_gss_final = 0.0
                        elif tipo == 'D': 
                            utilidad_gss_final = gss_logistica + impuesto_gss - amt_due_cxp
                        
                        return pd.Series([utilidad_gss_final, utilidad_socio_final], index=['utilidad_gss', 'utilidad_socio'])

                    df_processed[['utilidad_gss', 'utilidad_socio']] = df_processed.apply(apply_final_profit_calculation, axis=1)


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
                        'impuesto_gss', 'utilidad_gss', 'utilidad_socio', 'peso_kg',
                        # Add any other numeric columns that might be added during processing and need to be summed
                        'amt_due_cxp', 'arancel_cxp', 'iva_cxp'
                    ]
                    # Definir las columnas a tomar la primera aparición (para campos no sumables)
                    first_cols = [
                        'account_name', 'serial', 'asignacion', 'pais', 
                        'tipo_calculo', 'moneda', 'logistic_type', 'order_status_meli',
                        'date_created', 'system', 'etiqueta_envio', 'refunded_date'
                    ]
                    
                    # Asegurar que solo las columnas que existen en df_processed se incluyan en la agregación
                    agg_numeric_cols = {col: 'sum' for col in numeric_cols if col in df_processed.columns}
                    agg_first_cols = {col: 'first' for col in first_cols if col in df_processed.columns}
                    
                    # Unir los diccionarios de agregación
                    agg_dict = {**agg_numeric_cols, **agg_first_cols}

                    st.write("DEBUG: Diccionario de agregación para Supabase:", agg_dict)
                    
                    # Realizar la agregación
                    df_to_save_to_supabase = df_processed.groupby('order_id').agg(agg_dict).reset_index()

                    st.write("DEBUG: Columnas de df_to_save_to_supabase antes de guardar:", df_to_save_to_supabase.columns.tolist())
                    st.write("DEBUG: ¿Hay duplicados en order_id en df_to_save_to_supabase?", df_to_save_to_supabase['order_id'].duplicated().any())
                    if df_to_save_to_supabase['order_id'].duplicated().any():
                        st.error("DEBUG CRITICAL: ¡Hay order_id duplicados en el DataFrame a guardar! Esto causará el error ON CONFLICT.")
                        st.write("DEBUG: Duplicados:", df_to_save_to_supabase[df_to_save_to_supabase['order_id'].duplicated(keep=False)])


                    # Guardar en Supabase el DataFrame agregado
                    save_success, save_message = save_orders_to_supabase(df_to_save_to_supabase)
                    if save_success:
                        st.success(f"💾 {save_message}")
                    else:
                        st.error(f"❌ Error al guardar en Supabase: {save_message}")
                    
                    # Limpiar cache para force the dashboard to reload
                    st.cache_data.clear()
                    
                    # Mostrar resumen de lo procesado
                    st.markdown("### 📊 Resumen de Procesamiento")
                    col_p1, col_p2, col_p3, col_p4 = st.columns(4)
                    with col_p1:
                        st.metric("Total Órdenes Procesadas", len(df_processed))
                    with col_p2:
                        st.metric("Utilidad GSS Calculada", f"${df_processed['utilidad_gss'].sum():,.2f}")
                    with col_p3:
                        st.metric("Utilidad Socio Calculada", f"${df_processed['utilidad_socio'].sum():,.2f}")
                    with col_p4:
                        st.metric("Tiendas Procesadas", df_processed['account_name'].nunique())
                    
                    st.markdown("### 👀 Vista Previa de Datos Procesados (Primeras 10 filas)")
                    st.dataframe(df_processed[['order_id', 'account_name', 'pais', 'meli_usd', 'costo_amazon', 
                                                'total_anican', 'aditional', 'costo_cxp', 'impuesto_gss',
                                                'gss_logistica', 'utilidad_gss', 'utilidad_socio']].head(10))

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
        • <strong>Socio_cuenta:</strong> 0 si order_status_meli = "refunded", sino 1<br>
        • <strong>Asignacion:</strong> prefijo + Serial# para unir con CXP<br><br>
        
        <h5>🌍 Países aplicables:</h5>
        • Chile (MEGATIENDA SPA)<br>
        • Colombia (VEENDELO)
        </div>
        """, unsafe_allow_html=True)
    
    with tab3:
        st.markdown("### Tipo C: DETODOPARATODOS, COMPRAFACIL, COMPRA-YA")
        st.markdown("""
        <div class="formula-box">
        <h4>📐 Fórmula Principal</h4>
        <strong>Utilidad Gss = MELI USD - Costo Amazon - Total - Aditional - Impuesto por facturación</strong><br><br>
        
        <h5>🔧 Lógica Especial:</h5>
        • <strong>Impuesto por facturación:</strong> 1 si order_status_meli = "approved" o "in mediation", sino 0<br>
        • <strong>Utilidad Socio:</strong> 7.5 si Utilidad > 7.5, sino Utilidad<br>
        • <strong>Si Utilidad > 7.5:</strong> Utilidad Gss = Utilidad - Utilidad Socio<br>
        • <strong>Si Utilidad ≤ 7.5:</strong> Utilidad Gss = 0<br><br>
        
        <h5>🌍 Países aplicables:</h5>
        • Colombia (todas las tiendas tipo C)
        </div>
        """, unsafe_allow_html=True)
    
    with tab4:
        st.markdown("### Tipo D: FABORCARGO")
        st.markdown("""
        <div class="formula-box">
        <h4>📐 Fórmula Principal</h4>
        <strong>Utilidad Gss = Gss Logística + Impuesto Gss - Amt. Due</strong><br><br>
        
        <h5>🔧 Componentes:</h5>
        • <strong>Peso:</strong> logistic_weight_lbs × quantity × 0.453592 (conversión a kg)<br>
        • <strong>Gss Logística:</strong> según tabla ANEXO A por peso en kg<br>
        • <strong>Impuesto Gss:</strong> Arancel + IVA (del archivo CXP)<br>
        • <strong>Bodegal:</strong> 3.5 si logistic_type = "xd_drop_off", sino 0<br><br>
        
        <h5>🌍 Países aplicables:</h5>
        • Colombia (FABORCARGO)
        </div>
        """, unsafe_allow_html=True)

# ============================
# FOOTER
# ============================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 1rem;'>
    <p><strong>Sistema Contable Multi-País v3.0</strong> | Powered by Streamlit + Supabase</p>
    <p>🌎 Gestión financiera unificada para Colombia, Perú y Chile</p>
</div>
""", unsafe_allow_html=True)

def process_anican_logistics_complete(df_processed, anican_file):
    try:
        df_anican = pd.read_excel(anican_file)
        
        st.info(f"🔍 Columnas detectadas en Anican Logistics: {list(df_anican.columns)}")
        
        # Buscar columna Reference para el merge
        reference_col = None
        for col in df_anican.columns:
            if str(col).strip() == 'Reference':
                reference_col = col
                break
        
        if reference_col:
            st.success(f"✅ Columna Reference encontrada: '{reference_col}'")
            
            # Preparar TODAS las columnas para el merge
            df_anican_clean = df_anican.copy()
            df_anican_clean[reference_col] = df_anican_clean[reference_col].astype(str).str.strip()
            
            # Renombrar Reference a order_id para el merge, mantener todas las demás
            df_anican_clean = df_anican_clean.rename(columns={reference_col: 'order_id'})
            
            # Agregar prefijo 'anican_' a todas las columnas excepto order_id
            columns_to_rename = {}
            for col in df_anican_clean.columns:
                if col != 'order_id':
                    columns_to_rename[col] = f'anican_{col}'
            
            df_anican_clean = df_anican_clean.rename(columns=columns_to_rename)
            
            # Hacer merge con TODAS las columnas
            df_processed = df_processed.merge(
                df_anican_clean,
                on='order_id',
                how='left'
            )
            
            matches = df_processed[f'anican_{list(columns_to_rename.keys())[0]}'].notna().sum()
            st.success(f"✅ Anican Logistics procesado: {matches} coincidencias, {len(df_anican_clean.columns)-1} columnas añadidas")
            
        else:
            st.error(f"❌ No se encontró columna 'Reference' en Anican Logistics")
            st.info("Columnas disponibles: " + ", ".join(df_anican.columns))
            
    except Exception as e:
        st.error(f"❌ Error procesando Anican Logistics: {str(e)}")
    
    return df_processed

def process_anican_aditionals_complete(df_processed, aditionals_file):
    try:
        df_aditionals = pd.read_excel(aditionals_file)
        
        st.info(f"🔍 Columnas detectadas en Anican Aditionals: {list(df_aditionals.columns)}")
        
        # Buscar columna Order Id para el merge
        order_id_col = None
        for col in df_aditionals.columns:
            if str(col).strip() == 'Order Id':
                order_id_col = col
                break
        
        if order_id_col:
            st.success(f"✅ Columna Order Id encontrada: '{order_id_col}'")
            
            # Preparar TODAS las columnas
            df_aditionals_clean = df_aditionals.copy()
            df_aditionals_clean[order_id_col] = df_aditionals_clean[order_id_col].astype(str).str.strip()
            
            # Renombrar Order Id a order_id para el merge
            df_aditionals_clean = df_aditionals_clean.rename(columns={order_id_col: 'order_id'})
            
            # Como puede haber múltiples líneas por order_id, agrupamos manteniendo la info
            # Para campos de texto, tomamos el primero; para numéricos, sumamos
            agg_dict = {}
            for col in df_aditionals_clean.columns:
                if col != 'order_id':
                    if df_aditionals_clean[col].dtype in ['int64', 'float64']:
                        agg_dict[col] = 'sum'
                    else:
                        agg_dict[col] = 'first'
            
            df_aditionals_grouped = df_aditionals_clean.groupby('order_id').agg(agg_dict).reset_index()
            
            # Agregar prefijo 'aditional_' a todas las columnas excepto order_id
            columns_to_rename = {}
            for col in df_aditionals_grouped.columns:
                if col != 'order_id':
                    columns_to_rename[col] = f'aditional_{col}'
            
            df_aditionals_grouped = df_aditionals_grouped.rename(columns=columns_to_rename)
            
            # Hacer merge
            df_processed = df_processed.merge(
                df_aditionals_grouped,
                on='order_id',
                how='left'
            )
            
            matches = df_processed[f'aditional_{list(columns_to_rename.keys())[0]}'].notna().sum()
            st.success(f"✅ Anican Aditionals procesado: {matches} coincidencias, {len(df_aditionals_grouped.columns)-1} columnas añadidas")
            
        else:
            st.error(f"❌ No se encontró columna 'Order Id' en Anican Aditionals")
            st.info("Columnas disponibles: " + ", ".join(df_aditionals.columns))
            
    except Exception as e:
        st.error(f"❌ Error procesando Anican Aditionals: {str(e)}")
    
    return df_processed

def process_cxp_complete(df_processed, cxp_file):
    try:
        excel_file_cxp = pd.ExcelFile(cxp_file)
        df_cxp = None
        
        for sheet_name in excel_file_cxp.sheet_names:
            for header_row in range(5):
                try:
                    df_test_cxp = pd.read_excel(cxp_file, sheet_name=sheet_name, header=header_row)
                    
                    st.info(f"🔍 Probando hoja '{sheet_name}', fila {header_row}")
                    
                    # Buscar columna Ref # para el merge
                    ref_col = None
                    for col in df_test_cxp.columns:
                        if str(col).strip() == 'Ref #':
                            ref_col = col
                            break
                    
                    if ref_col:
                        st.success(f"✅ CXP: Columna Ref # encontrada en '{sheet_name}', fila {header_row}")
                        
                        # Tomar TODAS las columnas
                        df_cxp = df_test_cxp.copy()
                        df_cxp[ref_col] = df_cxp[ref_col].astype(str).str.strip()
                        
                        # Renombrar Ref # a asignacion_cxp para el merge
                        df_cxp = df_cxp.rename(columns={ref_col: 'asignacion_cxp'})
                        
                        # Agregar prefijo 'cxp_' a todas las columnas excepto asignacion_cxp
                        columns_to_rename = {}
                        for col in df_cxp.columns:
                            if col != 'asignacion_cxp':
                                columns_to_rename[col] = f'cxp_{col}'
                        
                        df_cxp = df_cxp.rename(columns=columns_to_rename)
                        
                        break
                        
                except Exception:
                    continue
                    
            if df_cxp is not None:
                break
        
        if df_cxp is not None:
            # Hacer merge usando asignacion
            df_processed = df_processed.merge(
                df_cxp,
                left_on='Asignacion',  # Asumiendo que ya existe en df_processed
                right_on='asignacion_cxp',
                how='left'
            )
            
            # Limpiar columna temporal
            df_processed = df_processed.drop(columns=['asignacion_cxp'])
            
            matches = df_processed[f'cxp_{list(columns_to_rename.keys())[0]}'].notna().sum()
            st.success(f"✅ Chile Express procesado: {matches} coincidencias, {len(df_cxp.columns)-1} columnas añadidas")
            
        else:
            st.warning("⚠️ No se pudo encontrar columna 'Ref #' en el archivo CXP")
                
    except Exception as e:
        st.error(f"❌ Error procesando Chile Express: {str(e)}")
    
    return df_processed

def save_orders_complete_to_supabase(df_processed_for_save):
    """Guarda órdenes con TODAS las columnas en Supabase"""
    try:
        supabase = init_supabase()
        if not supabase:
            return False, "No hay conexión a Supabase"
        
        orders_data = []
        for _, row in df_processed_for_save.iterrows():
            # Crear diccionario con TODAS las columnas
            order_dict = {}
            for col in df_processed_for_save.columns:
                value = row.get(col)
                
                # Convertir según tipo de dato
                if pd.isna(value):
                    order_dict[col] = None
                elif isinstance(value, (int, float)):
                    order_dict[col] = float(value) if not pd.isna(value) else 0.0
                elif isinstance(value, datetime):
                    order_dict[col] = value.isoformat()
                else:
                    order_dict[col] = str(value)
            
            orders_data.append(order_dict)
            
        # Insertar en lotes
        batch_size = 50  # Reducido porque hay más columnas
        total_inserted = 0
        
        for i in range(0, len(orders_data), batch_size):
            batch = orders_data[i:i+batch_size]
            result = supabase.table('orders_complete').upsert(batch, on_conflict='order_id').execute()
            total_inserted += len(batch)
            
        return True, f"{total_inserted} órdenes completas guardadas/actualizadas"
        
    except Exception as e:
        return False, f"Error al guardar órdenes completas: {str(e)}"
