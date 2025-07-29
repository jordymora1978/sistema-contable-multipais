import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
import time

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
    """Inicializa conexión con Supabase usando secrets"""
    try:
        url = st.secrets["supabase"]["url"]
        key = st.secrets["supabase"]["anon_key"]
        supabase: Client = create_client(url, key)
        return supabase
    except Exception as e:
        st.error(f"Error conectando a Supabase: {str(e)}")
        st.error("Verifica que las credenciales estén configuradas en Streamlit Cloud")
        return None

def test_connection():
    """Prueba la conexión con Supabase"""
    try:
        supabase = init_supabase()
        if supabase:
            result = supabase.table('store_config').select('count').execute()
            return True, "✅ Conectado a Supabase"
        return False, "❌ No se pudo inicializar Supabase"
    except Exception as e:
        return False, f"❌ Error: {str(e)}"

def get_store_config():
    """Obtiene configuración de tiendas desde Supabase"""
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
        return {}
    except Exception as e:
        st.error(f"Error obteniendo configuración: {str(e)}")
        return {}

def get_trm_rates():
    """Obtiene las últimas tasas TRM"""
    try:
        supabase = init_supabase()
        if supabase:
            result = supabase.table('trm_rates').select('*').order('date_updated', desc=True).execute()
            if result.data:
                trm_dict = {}
                seen_currencies = set()
                for row in result.data:
                    if row['currency'] not in seen_currencies:
                        trm_dict[row['currency']] = row['rate']
                        seen_currencies.add(row['currency'])
                return trm_dict
        return {'COP': 4000.0, 'PEN': 3.8, 'CLP': 900.0}
    except Exception as e:
        st.error(f"Error obteniendo TRM: {str(e)}")
        return {'COP': 4000.0, 'PEN': 3.8, 'CLP': 900.0}

def save_trm_rates(trm_data):
    """Guarda tasas TRM en Supabase"""
    try:
        supabase = init_supabase()
        if supabase:
            for currency, rate in trm_data.items():
                if currency != 'last_update':
                    data = {
                        'currency': currency,
                        'rate': float(rate),
                        'updated_by': 'streamlit_app'
                    }
                    supabase.table('trm_rates').insert(data).execute()
            return True, "TRM guardado exitosamente"
        return False, "No se pudo conectar a Supabase"
    except Exception as e:
        return False, f"Error: {str(e)}"

def save_orders_to_supabase(df):
    """Guarda órdenes procesadas en Supabase"""
    try:
        supabase = init_supabase()
        if not supabase:
            return False, "No hay conexión a Supabase"
        
        orders_data = []
        for _, row in df.iterrows():
            order_dict = {
                'order_id': str(row.get('order_id', '')),
                'account_name': str(row.get('account_name', '')),
                'serial_number': str(row.get('Serial#', '')),
                'asignacion': str(row.get('Asignacion', '')),
                'pais': str(row.get('pais', '')),
                'tipo_calculo': str(row.get('tipo_calculo', '')),
                'moneda': str(row.get('moneda', '')),
                'date_created': row.get('date_created'),
                'quantity': int(row.get('quantity', 0)),
                'logistic_type': str(row.get('logistic_type', '')),
                'order_status_meli': str(row.get('order_status_meli', '')),
                'declare_value': float(row.get('Declare Value', 0)),
                'net_real_amount': float(row.get('net_real_amount', 0)),
                'logistic_weight_lbs': float(row.get('logistic_weight_lbs', 0)),
                'meli_usd': float(row.get('MELI USD', 0)),
                'costo_amazon': float(row.get('Costo Amazon', 0)),
                'total_anican': float(row.get('Total_Anican', 0)),
                'aditional': float(row.get('Aditional', 0)),
                'bodegal': float(row.get('Bodegal', 0)),
                'socio_cuenta': float(row.get('Socio_cuenta', 0)),
                'costo_cxp': float(row.get('Costo cxp', 0)),
                'impuesto_facturacion': float(row.get('Impuesto por facturacion', 0)),
                'gss_logistica': float(row.get('Gss Logistica', 0)),
                'impuesto_gss': float(row.get('Impuesto Gss', 0)),
                'utilidad_gss': float(row.get('Utilidad Gss', 0)),
                'utilidad_socio': float(row.get('Utilidad Socio', 0))
            }
            orders_data.append(order_dict)
        
        # Insertar en lotes
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(orders_data), batch_size):
            batch = orders_data[i:i+batch_size]
            result = supabase.table('orders').upsert(batch, on_conflict='order_id').execute()
            total_inserted += len(batch)
        
        return True, f"{total_inserted} órdenes guardadas"
    
    except Exception as e:
        return False, f"Error: {str(e)}"

def get_orders_from_supabase(limit=1000):
    """Obtiene órdenes desde Supabase"""
    try:
        supabase = init_supabase()
        if supabase:
            result = supabase.table('orders').select('*').order('created_at', desc=True).limit(limit).execute()
            if result.data:
                return True, pd.DataFrame(result.data)
        return False, "No hay datos"
    except Exception as e:
        return False, f"Error: {str(e)}"

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
    
    return 373.72

def calcular_utilidades(df, store_config, trm_data):
    """Calcula utilidades según tipo de tienda"""
    
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
    
    # Inicializar columnas
    for col in ['Aditional', 'Bodegal', 'Socio_cuenta', 'Impuesto por facturacion', 
               'Gss Logistica', 'Impuesto Gss', 'Utilidad Gss', 'Utilidad Socio',
               'Total_Anican', 'Costo cxp']:
        df[col] = 0
    
    df['Costo Amazon'] = df['Declare Value'].fillna(0) * df['quantity'].fillna(1)
    
    # Calcular utilidades por tipo
    for idx, row in df.iterrows():
        tipo = row['tipo_calculo']
        
        if tipo == 'A':  # TODOENCARGO-CO, MEGA TIENDAS PERUANAS
            df.at[idx, 'Utilidad Gss'] = (
                row['MELI USD'] - row['Costo Amazon'] - 
                row.get('Total_Anican', 0) - row.get('Aditional', 0)
            )
        
        elif tipo == 'B':  # MEGATIENDA SPA, VEENDELO
            df.at[idx, 'Bodegal'] = 3.5 if str(row.get('logistic_type', '')).lower() == 'xd_drop_off' else 0
            df.at[idx, 'Socio_cuenta'] = 0 if str(row.get('order_status_meli', '')).lower() == 'refunded' else 1
            df.at[idx, 'Utilidad Gss'] = (
                row['MELI USD'] - row.get('Costo cxp', 0) - 
                row['Costo Amazon'] - row.get('Bodegal', 0) - row.get('Socio_cuenta', 0)
            )
        
        elif tipo == 'C':  # DETODOPARATODOS, COMPRAFACIL, COMPRA-YA
            df.at[idx, 'Impuesto por facturacion'] = (
                1 if str(row.get('order_status_meli', '')).lower() in ['approved', 'in mediation'] else 0
            )
            
            utilidad_base = (
                row['MELI USD'] - row['Costo Amazon'] - 
                row.get('Total_Anican', 0) - row.get('Aditional', 0) - 
                row.get('Impuesto por facturacion', 0)
            )
            
            if utilidad_base > 7.5:
                df.at[idx, 'Utilidad Socio'] = 7.5
                df.at[idx, 'Utilidad Gss'] = utilidad_base - 7.5
            else:
                df.at[idx, 'Utilidad Socio'] = utilidad_base
                df.at[idx, 'Utilidad Gss'] = 0
        
        elif tipo == 'D':  # FABORCARGO
            peso_libras = (row.get('logistic_weight_lbs', 0) * row.get('quantity', 1)) if pd.notna(row.get('logistic_weight_lbs')) else 0
            peso_kg = peso_libras * 0.453592
            df.at[idx, 'Gss Logistica'] = obtener_gss_logistica(peso_kg)
            df.at[idx, 'Bodegal'] = 3.5 if str(row.get('logistic_type', '')).lower() == 'xd_drop_off' else 0
            
            df.at[idx, 'Utilidad Gss'] = (
                row.get('Gss Logistica', 0) + row.get('Impuesto Gss', 0) - row.get('Costo cxp', 0)
            )
    
    return df

# ============================
# INICIALIZAR SESSION STATE
# ============================

if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None

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
                st.metric("Total Órdenes", len(df))
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
        success, df = get_orders_from_supabase(500)
        
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
                    height=400
                )
                st.plotly_chart(fig_pais, use_container_width=True)
            
            with tab2:
                # Top tiendas
                tienda_data = df.groupby('account_name').agg({
                    'utilidad_gss': 'sum',
                    'utilidad_socio': 'sum'
                }).reset_index()
                tienda_data['total'] = tienda_data['utilidad_gss'] + tienda_data['utilidad_socio']
                tienda_data = tienda_data.sort_values('total', ascending=True).tail(10)
                
                fig_tienda = px.bar(
                    tienda_data, 
                    x='total', 
                    y='account_name',
                    orientation='h',
                    title='Top 10 Tiendas por Utilidad Total',
                    height=500
                )
                st.plotly_chart(fig_tienda, use_container_width=True)
            
            # Tabla de datos
            st.markdown("### 📋 Datos Recientes")
            st.dataframe(
                df[['order_id', 'account_name', 'pais', 'meli_usd', 'utilidad_gss', 'utilidad_socio']].head(20),
                use_container_width=True
            )
        
        else:
            st.warning("No hay datos disponibles en Supabase")
    
    except Exception as e:
        st.error(f"Error cargando dashboard: {str(e)}")

elif page == "📁 Procesar Archivos":
    st.header("📁 Procesar Archivos")
    
    st.markdown("""
    <div class="info-box">
        <h4>🔄 Proceso de Carga</h4>
        <p>Sube tu archivo DRAPIFY y el sistema calculará automáticamente las utilidades según las fórmulas de negocio.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Formulario de carga
    drapify_file = st.file_uploader(
        "📄 Archivo DRAPIFY (Orders_XXXXXXXX)",
        type=['xlsx', 'xls'],
        help="Archivo principal con órdenes de MercadoLibre"
    )
    
    if st.button("🚀 Procesar y Guardar en Supabase", type="primary"):
        if drapify_file:
            try:
                with st.spinner("Procesando archivo..."):
                    # Leer archivo
                    df_drapify = pd.read_excel(drapify_file)
                    
                    # Verificar columnas básicas
                    columnas_requeridas = ['order_id', 'account_name', 'quantity', 'net_real_amount', 'Declare Value']
                    
                    columnas_faltantes = [col for col in columnas_requeridas if col not in df_drapify.columns]
                    
                    if columnas_faltantes:
                        st.error(f"❌ Faltan columnas: {', '.join(columnas_faltantes)}")
                        st.stop()
                    
                    # Procesar datos
                    df_procesado = calcular_utilidades(df_drapify, st.session_state.store_config, st.session_state.trm_data)
                    
                    # Guardar en Supabase
                    success, message = save_orders_to_supabase(df_procesado)
                    
                    if success:
                        st.success(f"✅ {message}")
                        
                        # Mostrar resumen
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("📦 Órdenes Procesadas", len(df_procesado))
                        with col2:
                            st.metric("💰 Utilidad GSS", f"${df_procesado['Utilidad Gss'].sum():.2f}")
                        with col3:
                            st.metric("🤝 Utilidad Socio", f"${df_procesado['Utilidad Socio'].sum():.2f}")
                        with col4:
                            st.metric("🏪 Tiendas", df_procesado['account_name'].nunique())
                        
                        # Guardar en session state
                        st.session_state.processed_data = df_procesado
                        
                        st.markdown("### 👀 Vista Previa")
                        st.dataframe(df_procesado[['account_name', 'order_id', 'pais', 'MELI USD', 'Utilidad Gss', 'Utilidad Socio']].head(10))
                    
                    else:
                        st.error(f"❌ Error: {message}")
            
            except Exception as e:
                st.error(f"❌ Error procesando archivo: {str(e)}")
        
        else:
            st.warning("⚠️ Por favor, sube un archivo DRAPIFY")

elif page == "💱 Configurar TRM":
    st.header("💱 Configuración de Tasas de Cambio")
    
    with st.form("trm_form"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("### 🇨🇴 Colombia")
            cop_trm = st.number_input(
                "COP por 1 USD",
                value=float(st.session_state.trm_data.get('COP', 4000.0)),
                step=50.0,
                min_value=1000.0,
                max_value=10000.0
            )
        
        with col2:
            st.markdown("### 🇵🇪 Perú")
            pen_trm = st.number_input(
                "PEN por 1 USD",
                value=float(st.session_state.trm_data.get('PEN', 3.8)),
                step=0.1,
                min_value=1.0,
                max_value=10.0
            )
        
        with col3:
            st.markdown("### 🇨🇱 Chile")
            clp_trm = st.number_input(
                "CLP por 1 USD",
                value=float(st.session_state.trm_data.get('CLP', 900.0)),
                step=10.0,
                min_value=500.0,
                max_value=1500.0
            )
        
        submitted = st.form_submit_button("💾 Actualizar TRM", type="primary")
    
    if submitted:
        try:
            new_trm_data = {
                'COP': cop_trm,
                'PEN': pen_trm,
                'CLP': clp_trm,
                'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            st.session_state.trm_data.update(new_trm_data)
            
            # Guardar en Supabase
            success, message = save_trm_rates(new_trm_data)
            
            if success:
                st.success("✅ Tasas TRM actualizadas exitosamente!")
                st.rerun()
            else:
                st.error(f"❌ Error: {message}")
        
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
