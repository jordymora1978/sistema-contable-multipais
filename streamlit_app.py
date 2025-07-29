import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import json
import re
import os
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import psycopg2

# Configuración de tiendas actualizada
TIENDAS_CONFIG = {
    '1-TODOENCARGO-CO': {'prefijo': 'TDC', 'pais': 'Colombia', 'tipo_calculo': 'A'},
    '2-MEGATIENDA SPA': {'prefijo': 'MEGA', 'pais': 'Chile', 'tipo_calculo': 'B'},
    '3-VEENDELO': {'prefijo': 'VEEN', 'pais': 'Colombia', 'tipo_calculo': 'B'},
    '4-MEGA TIENDAS PERUANAS': {'prefijo': 'MGA-PE', 'pais': 'Perú', 'tipo_calculo': 'A'},
    '5-DETODOPARATODOS': {'prefijo': 'DTPT', 'pais': 'Colombia', 'tipo_calculo': 'C'},
    '6-COMPRAFACIL': {'prefijo': 'CFA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
    '7-COMPRA-YA': {'prefijo': 'CPYA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
    '8-FABORCARGO': {'prefijo': 'FBC', 'pais': 'Colombia', 'tipo_calculo': 'D'}
}

# Tabla ANEXO A para cálculo de Gss Logística
ANEXO_A_GSS_LOGISTICA = [
    (0.01, 0.50, 24.01), (0.51, 1.00, 33.09), (1.01, 1.50, 42.17),
    (1.51, 2.00, 51.25), (2.01, 2.50, 61.94), (2.51, 3.00, 71.02),
    (3.01, 3.50, 80.91), (3.51, 4.00, 89.99), (4.01, 4.50, 99.87),
    (4.51, 5.00, 108.95), (5.01, 5.50, 117.19), (5.51, 6.00, 126.12),
    (6.01, 6.50, 135.85), (6.51, 7.00, 144.78), (7.01, 7.50, 154.52),
    (7.51, 8.00, 163.75), (8.01, 8.50, 173.18), (8.51, 9.00, 182.11),
    (9.01, 9.50, 191.85), (9.51, 10.00, 200.78), (10.01, 10.50, 207.36),
    (10.51, 11.00, 216.14), (11.01, 11.50, 225.73), (11.51, 12.00, 234.51),
    (12.01, 12.50, 244.09), (12.51, 13.00, 252.87), (13.01, 13.50, 262.46),
    (13.51, 14.00, 271.24), (14.01, 14.50, 280.82), (14.51, 15.00, 289.60),
    (15.01, 15.50, 294.54), (15.51, 16.00, 303.17), (16.01, 16.50, 312.60),
    (16.51, 17.00, 321.23), (17.01, 17.50, 330.67), (17.51, 18.00, 339.30),
    (18.01, 18.50, 348.73), (18.51, 19.00, 357.36), (19.01, 19.50, 366.80),
    (19.51, 20.00, 373.72)
]

# ========================
# FUNCIONES DE BASE DE DATOS
# ========================

@st.cache_resource
def get_database_connection():
    """Conectar a la base de datos de Replit"""
    try:
        # URL de conexión de Replit Database
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            st.error("❌ No se encontró la configuración de base de datos")
            return None
        
        engine = create_engine(database_url)
        return engine
    except Exception as e:
        st.error(f"❌ Error conectando a la base de datos: {str(e)}")
        return None

def crear_tablas():
    """Crear tablas necesarias en la base de datos"""
    engine = get_database_connection()
    if engine is None:
        return False
    
    try:
        with engine.connect() as conn:
            # Tabla principal de órdenes
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ordenes (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(50) UNIQUE NOT NULL,
                    system_id VARCHAR(50),
                    serial_number VARCHAR(50),
                    account_name VARCHAR(100),
                    fecha_creacion TIMESTAMP,
                    cantidad INTEGER,
                    logistic_type VARCHAR(50),
                    order_status_meli VARCHAR(50),
                    declare_value DECIMAL(12,2),
                    net_real_amount DECIMAL(12,2),
                    logistic_weight_lbs DECIMAL(10,2),
                    asignacion VARCHAR(50),
                    pais VARCHAR(50),
                    tipo_calculo VARCHAR(10),
                    moneda VARCHAR(10),
                    costo_amazon DECIMAL(12,2),
                    meli_usd DECIMAL(12,2),
                    aditional DECIMAL(12,2),
                    bodegal DECIMAL(12,2),
                    socio_cuenta INTEGER,
                    impuesto_facturacion INTEGER,
                    gss_logistica DECIMAL(12,2),
                    impuesto_gss DECIMAL(12,2),
                    total_anican DECIMAL(12,2),
                    costo_cxp DECIMAL(12,2),
                    utilidad_gss DECIMAL(12,2),
                    utilidad_socio DECIMAL(12,2),
                    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Tabla de TRM histórico
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS trm_historico (
                    id SERIAL PRIMARY KEY,
                    fecha DATE DEFAULT CURRENT_DATE,
                    cop_usd DECIMAL(8,2),
                    pen_usd DECIMAL(8,2),
                    clp_usd DECIMAL(8,2)
                )
            """))
            
            conn.commit()
        
        return True
    except Exception as e:
        st.error(f"❌ Error creando tablas: {str(e)}")
        return False

def guardar_ordenes_bd(df):
    """Guardar nuevas órdenes en la base de datos"""
    engine = get_database_connection()
    if engine is None:
        return False
    
    try:
        # Preparar datos para la BD
        df_bd = df.copy()
        
        # Renombrar columnas para coincidir con la BD
        columnas_bd = {
            'order_id': 'order_id',
            'System#': 'system_id',
            'Serial#': 'serial_number',
            'account_name': 'account_name',
            'date_created': 'fecha_creacion',
            'quantity': 'cantidad',
            'logistic_type': 'logistic_type',
            'order_status_meli': 'order_status_meli',
            'Declare Value': 'declare_value',
            'net_real_amount': 'net_real_amount',
            'logistic_weight_lbs': 'logistic_weight_lbs',
            'Asignacion': 'asignacion',
            'pais': 'pais',
            'tipo_calculo': 'tipo_calculo',
            'moneda': 'moneda',
            'Costo Amazon': 'costo_amazon',
            'MELI USD': 'meli_usd',
            'Aditional': 'aditional',
            'Bodegal': 'bodegal',
            'Socio_cuenta': 'socio_cuenta',
            'Impuesto por facturacion': 'impuesto_facturacion',
            'Gss Logistica': 'gss_logistica',
            'Impuesto Gss': 'impuesto_gss',
            'Total_Anican': 'total_anican',
            'Costo cxp': 'costo_cxp',
            'Utilidad Gss': 'utilidad_gss',
            'Utilidad Socio': 'utilidad_socio'
        }
        
        # Filtrar y renombrar columnas que existen
        df_bd_clean = pd.DataFrame()
        for col_original, col_bd in columnas_bd.items():
            if col_original in df_bd.columns:
                df_bd_clean[col_bd] = df_bd[col_original]
        
        # Evitar duplicados
        ordenes_existentes = leer_ordenes_existentes()
        if not ordenes_existentes.empty:
            df_bd_clean = df_bd_clean[~df_bd_clean['order_id'].isin(ordenes_existentes['order_id'])]
        
        if len(df_bd_clean) > 0:
            # Guardar en BD
            df_bd_clean.to_sql('ordenes', engine, if_exists='append', index=False, method='multi')
            return len(df_bd_clean)
        else:
            return 0
            
    except Exception as e:
        st.error(f"❌ Error guardando en BD: {str(e)}")
        return False

def leer_ordenes_existentes():
    """Leer órdenes existentes de la base de datos"""
    engine = get_database_connection()
    if engine is None:
        return pd.DataFrame()
    
    try:
        return pd.read_sql("SELECT * FROM ordenes", engine)
    except Exception as e:
        st.error(f"❌ Error leyendo BD: {str(e)}")
        return pd.DataFrame()

def leer_datos_por_periodo(fecha_inicio=None, fecha_fin=None):
    """Leer datos filtrados por período"""
    engine = get_database_connection()
    if engine is None:
        return pd.DataFrame()
    
    try:
        query = "SELECT * FROM ordenes"
        params = {}
        
        if fecha_inicio and fecha_fin:
            query += " WHERE fecha_creacion BETWEEN %(fecha_inicio)s AND %(fecha_fin)s"
            params = {'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin}
        
        query += " ORDER BY fecha_creacion DESC"
        
        return pd.read_sql(query, engine, params=params)
    except Exception as e:
        st.error(f"❌ Error leyendo datos por período: {str(e)}")
        return pd.DataFrame()

def guardar_trm_historico(cop, pen, clp):
    """Guardar TRM histórico"""
    engine = get_database_connection()
    if engine is None:
        return False
    
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO trm_historico (cop_usd, pen_usd, clp_usd) 
                VALUES (:cop, :pen, :clp)
            """), {'cop': cop, 'pen': pen, 'clp': clp})
            conn.commit()
        return True
    except Exception as e:
        st.error(f"❌ Error guardando TRM: {str(e)}")
        return False

# ========================
# FUNCIONES DE CÁLCULO (mantener las existentes)
# ========================

def calcular_asignacion(account_name, serial_number):
    if pd.isna(account_name) or pd.isna(serial_number):
        return None
    account_str = str(account_name).strip()
    prefijo = TIENDAS_CONFIG.get(account_str, {}).get('prefijo', '')
    if prefijo:
        return f"{prefijo}{serial_number}"
    return None

def obtener_gss_logistica(peso_kg):
    if pd.isna(peso_kg) or peso_kg <= 0:
        return 0
    for desde, hasta, gss_value in ANEXO_A_GSS_LOGISTICA:
        if desde <= peso_kg <= hasta:
            return gss_value
    return 373.72

def convertir_libras_a_kg(libras):
    if pd.isna(libras):
        return 0
    return libras * 0.453592

def calcular_bodega(logistic_type):
    if pd.isna(logistic_type):
        return 0
    return 3.5 if str(logistic_type).lower() == 'xd_drop_off' else 0

def calcular_socio_cuenta(order_status_meli):
    if pd.isna(order_status_meli):
        return 0
    return 0 if str(order_status_meli).lower() == 'refunded' else 1

def calcular_impuesto_facturacion(order_status_meli):
    if pd.isna(order_status_meli):
        return 0
    status = str(order_status_meli).lower()
    return 1 if status in ['approved', 'in mediation'] else 0

# ========================
# CONFIGURACIÓN DE LA APLICACIÓN
# ========================

st.set_page_config(
    page_title="Sistema Contable Multi-País DB",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado
st.markdown("""
<style>
.metric-card {
    background-color: #f0f2f6;
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #1f77b4;
}
.country-header {
    color: #1f77b4;
    border-bottom: 2px solid #1f77b4;
    padding-bottom: 0.5rem;
    margin-bottom: 1rem;
}
.success-box {
    background-color: #d4edda;
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #28a745;
    margin: 1rem 0;
}
</style>
""", unsafe_allow_html=True)

# Inicializar BD
if 'db_initialized' not in st.session_state:
    with st.spinner("🔧 Inicializando base de datos..."):
        if crear_tablas():
            st.session_state.db_initialized = True
            st.success("✅ Base de datos inicializada correctamente")
        else:
            st.error("❌ Error inicializando base de datos")

# Inicializar TRM
if 'trm_data' not in st.session_state:
    st.session_state.trm_data = {
        'COP': 4000.0,
        'PEN': 3.8,
        'CLP': 900.0,
        'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

# Título principal
st.title("🌎 Sistema Contable Multi-País con Base de Datos")
st.markdown("**Gestión financiera unificada con almacenamiento persistente**")

# Sidebar
st.sidebar.title("🎛️ Panel de Control")
pagina = st.sidebar.selectbox("Selecciona una sección:", [
    "📊 Dashboard Histórico",
    "📁 Cargar Nuevos Datos", 
    "💱 Configurar TRM",
    "📈 Análisis Avanzado",
    "🗄️ Gestión de Base de Datos"
])

# Mostrar TRM actual
st.sidebar.markdown("---")
st.sidebar.markdown("### 💱 TRM Actual")
for moneda, tasa in st.session_state.trm_data.items():
    if moneda != 'last_update':
        st.sidebar.metric(f"{moneda}/USD", f"{tasa:,.2f}")

# Mostrar estadísticas de BD
with st.sidebar:
    if st.session_state.get('db_initialized', False):
        try:
            ordenes_totales = len(leer_ordenes_existentes())
            st.sidebar.markdown("### 🗄️ Base de Datos")
            st.sidebar.metric("📦 Total Órdenes", ordenes_totales)
        except:
            st.sidebar.metric("📦 Total Órdenes", "Error")

# ========================
# PÁGINA: CARGAR NUEVOS DATOS
# ========================
if pagina == "📁 Cargar Nuevos Datos":
    st.header("📁 Cargar Nuevos Datos")
    
    st.markdown("""
    <div class="success-box">
    <strong>🆕 Modo Base de Datos Activado</strong><br>
    Los datos se guardarán permanentemente y se evitarán duplicados automáticamente.
    </div>
    """, unsafe_allow_html=True)
    
    # Crear columnas para organizar las cargas
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Archivos Principales")
        
        drapify_file = st.file_uploader(
            "DRAPIFY (Orders_XXXXXXXX)",
            type=['xlsx', 'xls'],
            key="drapify",
            help="Archivo principal con todas las órdenes de MercadoLibre"
        )
        
        anican_file = st.file_uploader(
            "Anican Logistics",
            type=['xlsx', 'xls'], 
            key="anican",
            help="Archivo con costos logísticos de Anican"
        )
        
        aditionals_file = st.file_uploader(
            "Anican Aditionals",
            type=['xlsx', 'xls'],
            key="aditionals", 
            help="Archivo con costos adicionales de Anican"
        )
    
    with col2:
        st.markdown("### 🚚 Archivos Logísticos")
        
        cxp_file = st.file_uploader(
            "Chile Express (CXP)",
            type=['xlsx', 'xls'],
            key="cxp",
            help="Archivo de Chile Express con costos logísticos"
        )
    
    # Procesar y guardar en BD
    if st.button("🔄 Procesar y Guardar en Base de Datos", type="primary"):
        if drapify_file:
            # Aquí va tu código de procesamiento actual...
            # (mantén toda la lógica existente)
            st.success("✅ ¡Implementa aquí el procesamiento completo!")
            
        else:
            st.warning("⚠️ Por favor, sube al menos el archivo DRAPIFY para continuar.")

# ========================
# PÁGINA: DASHBOARD HISTÓRICO
# ========================
elif pagina == "📊 Dashboard Histórico":
    st.header("📊 Dashboard Histórico")
    
    # Filtros de fecha
    col_fecha1, col_fecha2 = st.columns(2)
    with col_fecha1:
        fecha_inicio = st.date_input("📅 Fecha Inicio", value=datetime.now().replace(day=1))
    with col_fecha2:
        fecha_fin = st.date_input("📅 Fecha Fin", value=datetime.now())
    
    # Leer datos históricos
    df_historicos = leer_datos_por_periodo(fecha_inicio, fecha_fin)
    
    if not df_historicos.empty:
        # Métricas principales
        total_utilidad_gss = df_historicos['utilidad_gss'].sum()
        total_utilidad_socio = df_historicos['utilidad_socio'].sum()
        total_utilidad = total_utilidad_gss + total_utilidad_socio
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("💰 Utilidad Total", f"${total_utilidad:.2f} USD")
        with col2:
            st.metric("🏢 Utilidad Gss", f"${total_utilidad_gss:.2f} USD")
        with col3:
            st.metric("🤝 Utilidad Socio", f"${total_utilidad_socio:.2f} USD")
        with col4:
            st.metric("📦 Total Órdenes", len(df_historicos))
        
        # Gráficos y análisis con datos históricos
        st.markdown("### 📈 Tendencias Históricas")
        
        # Análisis por país
        pais_stats = df_historicos.groupby('pais').agg({
            'order_id': 'count',
            'meli_usd': 'sum',
            'utilidad_gss': 'sum',
            'utilidad_socio': 'sum'
        }).round(2)
        pais_stats.columns = ['Órdenes', 'Ingresos USD', 'Utilidad Gss', 'Utilidad Socio']
        pais_stats['Utilidad Total'] = pais_stats['Utilidad Gss'] + pais_stats['Utilidad Socio']
        
        st.dataframe(pais_stats)
        
    else:
        st.info("📅 No hay datos para el período seleccionado")

# ========================
# PÁGINA: GESTIÓN DE BD
# ========================
elif pagina == "🗄️ Gestión de Base de Datos":
    st.header("🗄️ Gestión de Base de Datos")
    
    # Estadísticas de BD
    ordenes_bd = leer_ordenes_existentes()
    
    if not ordenes_bd.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📦 Total Órdenes", len(ordenes_bd))
        with col2:
            primera_fecha = ordenes_bd['fecha_creacion'].min()
            st.metric("📅 Primera Orden", primera_fecha.strftime("%Y-%m-%d") if pd.notna(primera_fecha) else "N/A")
        with col3:
            ultima_fecha = ordenes_bd['fecha_creacion'].max()
            st.metric("📅 Última Orden", ultima_fecha.strftime("%Y-%m-%d") if pd.notna(ultima_fecha) else "N/A")
        
        # Exportar datos
        st.markdown("### 📤 Exportar Datos")
        if st.button("📥 Descargar Datos Completos (CSV)"):
            csv = ordenes_bd.to_csv(index=False)
            st.download_button(
                label="📁 Descargar CSV",
                data=csv,
                file_name=f"datos_completos_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        
        # Vista previa de datos
        st.markdown("### 👀 Vista Previa de Datos")
        st.dataframe(ordenes_bd.head(20))
        
    else:
        st.info("🗄️ La base de datos está vacía. Carga algunos datos primero.")

# Otras páginas...
else:
    st.info("🚧 Página en construcción...")

# Footer
st.markdown("---")
st.markdown(
    "<p style='text-align: center; color: #666;'>Sistema Contable Multi-País v3.0 con Base de Datos | "
    "Powered by Replit PostgreSQL</p>",
    unsafe_allow_html=True
)
