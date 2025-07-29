import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import json
import re
from supabase import create_client, Client

# --- Configuración Supabase ---
# Se recomienda usar st.secrets para almacenar credenciales de forma segura en Streamlit Cloud.
# Si ejecutas localmente, puedes crear un archivo .streamlit/secrets.toml
# [supabase]
# url = "https://your-supabase-url.supabase.co"
# key = "your-supabase-anon-key"
try:
    SUPABASE_URL = st.secrets["supabase"]["url"]
    SUPABASE_KEY = st.secrets["supabase"]["key"]
except KeyError:
    st.error("❌ Las credenciales de Supabase no se encontraron en st.secrets.")
    st.info("Por favor, configura 'supabase.url' y 'supabase.key' en .streamlit/secrets.toml")
    # Usar valores predeterminados para desarrollo local si no se configuran secretos
    SUPABASE_URL = "https://qzexuqkedukcwcyhrpza.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0" # Este es tu valor original, úsalo con precaución en producción
    
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Configuración de la app Streamlit ---
st.set_page_config(page_title="Sistema Contable Multi-País", layout="wide")
st.markdown("""
    <style>
        .main {background-color: #0e1117; color: white;} /* Fondo oscuro */
        .stApp {background-color: #0e1117; color: white;} /* Asegura que toda la app tenga el fondo oscuro */
        .stSelectbox > div {color: black !important;} /* Hace el texto del selectbox negro para visibilidad */
        .stDataFrame {
            background-color: #1a1a1a; /* Fondo más claro para el DataFrame */
            color: white; /* Texto blanco para el DataFrame */
            border-radius: 8px; /* Bordes redondeados */
            padding: 10px;
        }
        .stDataFrame table {
            color: white !important; /* Asegura que el texto de la tabla sea blanco */
        }
        .stDataFrame thead th {
            color: #4CAF50 !important; /* Color para los encabezados de columna */
        }
        /* Estilos para los botones */
        .stButton>button {
            background-color: #4CAF50; /* Un verde vibrante */
            color: white;
            border-radius: 12px;
            border: none;
            padding: 10px 20px;
            font-size: 16px;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
        }
        .stButton>button:hover {
            background-color: #45a049; /* Tono más oscuro al pasar el ratón */
            box_shadow: 0 6px 12px rgba(0, 0, 0, 0.3);
            transform: translateY(-2px);
        }
        .stTextInput>div>div>input {
            color: black !important;
            background-color: white !important;
        }
        .stNumberInput>div>div>input {
            color: black !important;
            background-color: white !important;
        }
        .metric-card {
            background-color: #2a2a2a; /* Fondo oscuro para la tarjeta de métrica */
            padding: 1rem;
            border-radius: 0.5rem;
            border-left: 4px solid #4CAF50; /* Borde verde */
            color: white;
        }
        .country-header {
            color: #4CAF50; /* Color verde para encabezados de país */
            border-bottom: 2px solid #4CAF50;
            padding-bottom: 0.5rem;
            margin-bottom: 1rem;
        }
        .formula-box {
            background-color: #1a1a1a; /* Fondo oscuro para caja de fórmula */
            padding: 1rem;
            border-radius: 0.5rem;
            border-left: 4px solid #28a745;
            margin: 1rem 0;
            color: white;
        }
    </style>
""", unsafe_allow_html=True)

# --- Configuración de tiendas actualizada (como en el código provisto) ---
TIENDAS_CONFIG = {
    '1-TODOENCARGO-CO': {
        'prefijo': 'TDC',
        'pais': 'Colombia',
        'tipo_calculo': 'A'
    },
    '2-MEGATIENDA SPA': {
        'prefijo': 'MEGA',
        'pais': 'Chile',
        'tipo_calculo': 'B'
    },
    '3-VEENDELO': {
        'prefijo': 'VEEN',
        'pais': 'Colombia',
        'tipo_calculo': 'B'
    },
    '4-MEGA TIENDAS PERUANAS': {
        'prefijo': 'MGA-PE',
        'pais': 'Perú',
        'tipo_calculo': 'A'
    },
    '5-DETODOPARATODOS': {
        'prefijo': 'DTPT',
        'pais': 'Colombia',
        'tipo_calculo': 'C'
    },
    '6-COMPRAFACIL': {
        'prefijo': 'CFA',
        'pais': 'Colombia',
        'tipo_calculo': 'C'
    },
    '7-COMPRA-YA': {
        'prefijo': 'CPYA',
        'pais': 'Colombia',
        'tipo_calculo': 'C'
    },
    '8-FABORCARGO': {
        'prefijo': 'FBC',
        'pais': 'Colombia',
        'tipo_calculo': 'D'
    }
}

# Tabla ANEXO A para cálculo de Gss Logística (como en el código provisto)
ANEXO_A_GSS_LOGISTICA = [(0.01, 0.50, 24.01), (0.51, 1.00, 33.09),
                         (1.01, 1.50, 42.17), (1.51, 2.00, 51.25),
                         (2.01, 2.50, 61.94), (2.51, 3.00, 71.02),
                         (3.01, 3.50, 80.91), (3.51, 4.00, 89.99),
                         (4.01, 4.50, 99.87), (4.51, 5.00, 108.95),
                         (5.01, 5.50, 117.19), (5.51, 6.00, 126.12),
                         (6.01, 6.50, 135.85), (6.51, 7.00, 144.78),
                         (7.01, 7.50, 154.52), (7.51, 8.00, 163.75),
                         (8.01, 8.50, 173.18), (8.51, 9.00, 182.11),
                         (9.01, 9.50, 191.85), (9.51, 10.00, 200.78),
                         (10.01, 10.50, 207.36), (10.51, 11.00, 216.14),
                         (11.01, 11.50, 225.73), (11.51, 12.00, 234.51),
                         (12.01, 12.50, 244.09), (12.51, 13.00, 252.87),
                         (13.01, 13.50, 262.46), (13.51, 14.00, 271.24),
                         (14.01, 14.50, 280.82), (14.51, 15.00, 289.60),
                         (15.01, 15.50, 294.54), (15.51, 16.00, 303.17),
                         (16.01, 16.50, 312.60), (16.51, 17.00, 321.23),
                         (17.01, 17.50, 330.67), (17.51, 18.00, 339.30),
                         (18.01, 18.50, 348.73), (18.51, 19.00, 357.36),
                         (19.01, 19.50, 366.80), (19.51, 20.00, 373.72)]

# --- Funciones de cálculo (como en el código provisto) ---

def calcular_asignacion(account_name, serial_number):
    """Calcula la columna Asignacion según las reglas del negocio."""
    if pd.isna(account_name) or pd.isna(serial_number):
        return None

    account_str = str(account_name).strip()
    prefijo = TIENDAS_CONFIG.get(account_str, {}).get('prefijo', '')

    if prefijo:
        return f"{prefijo}{serial_number}"
    return None


def obtener_gss_logistica(peso_kg):
    """Busca en ANEXO A el valor de Gss Logística según el peso en kg."""
    if pd.isna(peso_kg) or peso_kg <= 0:
        return 0

    for desde, hasta, gss_value in ANEXO_A_GSS_LOGISTICA:
        if desde <= peso_kg <= hasta:
            return gss_value

    # Si supera 20kg, usar el último valor (el de 20kg)
    return ANEXO_A_GSS_LOGISTICA[-1][2]


def convertir_libras_a_kg(libras):
    """Convierte libras a kilogramos."""
    if pd.isna(libras):
        return 0
    return libras * 0.453592


def calcular_bodega(logistic_type):
    """Calcula Bodegal según logistic_type."""
    if pd.isna(logistic_type):
        return 0
    return 3.5 if str(logistic_type).lower() == 'xd_drop_off' else 0


def calcular_socio_cuenta(order_status_meli):
    """Calcula Socio_cuenta según order_status_meli."""
    if pd.isna(order_status_meli):
        return 0
    return 0 if str(order_status_meli).lower() == 'refunded' else 1


def calcular_impuesto_facturacion(order_status_meli):
    """Calcula Impuesto por facturación."""
    if pd.isna(order_status_meli):
        return 0
    status = str(order_status_meli).lower()
    return 1 if status in ['approved', 'in mediation'] else 0

# --- Funciones de Supabase (las mismas que en la versión anterior) ---

# Función para obtener las TRM actuales
# Usa st.cache_data para evitar lecturas repetidas de Supabase si no hay cambios.
# @st.cache_data(ttl=600) # Caching por 10 minutos
def obtener_trm():
    try:
        # Intenta ordenar por 'created_at'. Si no existe, Supabase dará error.
        # Es CRÍTICO que la columna 'created_at' exista en tu tabla 'trm_rates' en Supabase
        # y que sea de tipo 'timestamp with time zone' con un default de 'now()'.
        # Si 'created_at' no existe, usaremos 'id' como fallback para el orden inicial.
        result = supabase.table("trm_rates").select("*").order("id", desc=True).execute() 
        df = pd.DataFrame(result.data)
        if not df.empty:
            # Convierte las columnas de fecha a datetime si existen
            for col in ['date_updated', 'created_at']: # Aún intentamos convertir created_at si el usuario lo añade después
                if col in df.columns:
                    # Usar infer_datetime_format=True para mejor compatibilidad de formatos ISO
                    df[col] = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
            # Asegúrate de que 'id' es numérico para el filtro de actualización
            if 'id' in df.columns:
                df['id'] = pd.to_numeric(df['id'], errors='coerce')
                # Eliminar filas con ID nulo si la conversión falla
                df = df.dropna(subset=['id'])
            # Ordenar por moneda y luego por 'created_at' si existe, si no por 'id'
            if 'created_at' in df.columns and not df['created_at'].isnull().all(): # Verifica si la columna existe y tiene valores
                 df = df.sort_values(by=['currency', 'created_at'], ascending=[True, False])
            else:
                 df = df.sort_values(by=['currency', 'id'], ascending=[True, False]) # Fallback para ordenar si created_at no está o es todo nulo
            df = df.drop_duplicates(subset=['currency'], keep='first')
        return df
    except Exception as e:
        st.error("❌ Error al leer datos de Supabase. Asegúrate que tu tabla 'trm_rates' tenga las columnas 'id', 'currency', 'rate', 'updated_by', 'date_updated', y **'created_at' (timestamp with time zone, default now())**.")
        st.exception(e)
        return pd.DataFrame()

# Función para añadir o actualizar una TRM
def guardar_trm(moneda, tasa, actualizado_por, editar_existente):
    fecha_actual = datetime.now().isoformat()

    try:
        # Siempre insertamos un nuevo registro para mantener el historial de TRM
        response = supabase.table("trm_rates").insert({
            "currency": moneda.upper(), # Guardar en mayúsculas
            "rate": tasa,
            "updated_by": actualizado_por,
            "date_updated": fecha_actual,
            "created_at": fecha_actual # Asegúrate de que esta columna exista en Supabase
        }).execute()
        st.success(f"✅ TRM para {moneda.upper()} guardada con éxito.")
        
        # Después de una operación de guardado exitosa, limpiar el caché y re-ejecutar la app
        st.cache_data.clear()
        st.rerun() # Esto recarga la página para mostrar los datos actualizados
            
    except Exception as e:
        st.error("❌ Error inesperado al guardar TRM.")
        st.exception(e)

# --- Inicialización de Session State ---
# Inicializar session state para TRM (ahora se carga desde Supabase, pero mantenemos fallback)
if 'trm_data' not in st.session_state:
    st.session_state.trm_data = {
        'COP': 4000.0,  # Pesos colombianos por USD
        'PEN': 3.8,     # Soles peruanos por USD
        'CLP': 900.0,   # Pesos chilenos por USD
        'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
# Cargar TRMs desde Supabase al iniciar la app o al refrescar
df_trm_supabase = obtener_trm()
if not df_trm_supabase.empty:
    for index, row in df_trm_supabase.iterrows():
        st.session_state.trm_data[row['currency']] = row['rate']
    st.session_state.trm_data['last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# Inicializar datos procesados
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None

# Título principal
st.title("🌎 Sistema Contable Multi-País - Versión Final Corregida")
st.markdown(
    "**Gestión financiera unificada con cálculos específicos por tipo de tienda**"
)

# Sidebar para navegación
st.sidebar.title("🎛️ Panel de Control")
pagina = st.sidebar.selectbox("Selecciona una sección:", [
    "📊 Dashboard", "💱 Configurar TRM", "📁 Procesar Archivos",
    "📈 Análisis de Utilidad", "📋 Fórmulas de Negocio"
])

# Mostrar TRM actual en sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("### 💱 TRM Actual")
for moneda, tasa in st.session_state.trm_data.items():
    if moneda != 'last_update':
        st.sidebar.metric(f"{moneda}/USD", f"{tasa:,.2f}")

st.sidebar.caption(
    f"Última actualización: {st.session_state.trm_data['last_update']}")


# ========================
# PÁGINA: FÓRMULAS DE NEGOCIO
# ========================
if pagina == "📋 Fórmulas de Negocio":
    st.header("📋 Fórmulas de Negocio por Tipo de Tienda")

    st.markdown("### Tipo A: TODOENCARGO-CO y MEGA TIENDAS PERUANAS")
    st.markdown("""
    <div class="formula-box">
    <strong>Utilidad Gss = MELI USD - Costo Amazon - Total - Aditional</strong><br>
    • Costo Amazon = Declare Value × quantity<br>
    • Aditional = Quantity × UnitPrice (del archivo Aditionals)<br>
    • MELI USD = net_real_amount / TRM<br>
    • Total = del archivo Anican Logistics
    </div>
    """,
                unsafe_allow_html=True)

    st.markdown("### Tipo B: MEGATIENDA SPA y VEENDELO")
    st.markdown("""
    <div class="formula-box">
    <strong>Utilidad Gss = MELI USD - Costo cxp - Costo Amazon - Bodegal - Socio_cuenta</strong><br>
    • Costo cxp = Amt. Due (del archivo Chile Express CXP)<br>
    • Bodegal = 3.5 si logistic_type = "xd_drop_off", sino 0<br>
    • Socio_cuenta = 0 si order_status_meli = "refunded", sino 1<br>
    • Asignacion = prefijo + Serial# para unir con CXP
    </div>
    """,
                unsafe_allow_html=True)

    st.markdown("### Tipo C: DETODOPARATODOS, COMPRAFACIL, COMPRA-YA")
    st.markdown("""
    <div class="formula-box">
    <strong>Utilidad Gss = MELI USD - Costo Amazon - Total - Aditional - Impuesto por facturación</strong><br>
    • Impuesto por facturación = 1 si order_status_meli = "approved" o "in mediation", sino 0<br>
    • Utilidad Socio = 7.5 si Utilidad > 7.5, sino Utilidad<br>
    • Si Utilidad > 7.5: Utilidad Gss = Utilidad - Utilidad Socio<br>
    • Si Utilidad ≤ 7.5: Utilidad Gss = 0
    </div>
    """,
                unsafe_allow_html=True)

    st.markdown("### Tipo D: FABORCARGO")
    st.markdown("""
    <div class="formula-box">
    <strong>Utilidad Gss = Gss Logística + Impuesto Gss - Amt. Due</strong><br>
    • Peso = logistic_weight_lbs × quantity × 0.453592 (conversión a kg)<br>
    • Gss Logística = según tabla ANEXO A por peso en kg<br>
    • Impuesto Gss = Arancel + IVA (del archivo CXP)<br>
    • Bodegal = 3.5 si logistic_type = "xd_drop_off", sino 0
    </div>
    """,
                unsafe_allow_html=True)

# ========================
# PÁGINA: CONFIGURAR TRM
# ========================
elif pagina == "💱 Configurar TRM":
    st.header("🔍 Gestión de Tasas de Cambio (TRM)")
    st.write("Aquí puedes ver las tasas de cambio actuales y añadir o actualizar nuevas.")

    # Mostrar TRMs actuales
    df_trm = obtener_trm()
    if not df_trm.empty:
        st.subheader("📊 TRMs Actuales")
        # Mostrar solo las columnas relevantes en un formato más limpio
        st.dataframe(df_trm.set_index('currency')[['rate', 'updated_by', 'date_updated']].style.format({"rate": "{:,.4f}"}))
    else:
        st.warning("⚠️ No se encontraron tasas de cambio (TRM). Agrega una nueva a continuación.")

    st.subheader("🛠️ Agregar o Actualizar TRM")
    
    # Usar un formulario para agrupar los inputs y el botón, mejora la UX
    with st.form("trm_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            moneda = st.text_input("Moneda (ej: COP, PEN, CLP)", max_chars=3).upper() # Asegura que sea mayúscula
        with col2:
            tasa = st.number_input("Tasa", min_value=0.0001, format="%.4f")
        
        actualizado_por = st.text_input("Actualizado por", value="jordy_mora")
        
        # El checkbox solo sirve como indicación, la función `guardar_trm` siempre inserta.
        editar_existente = st.checkbox("Esta es una nueva versión de una TRM existente (se añadirá un nuevo registro para esta moneda)")

        submitted = st.form_submit_button("💾 Guardar TRM")
        if submitted:
            if moneda and tasa > 0:
                guardar_trm(moneda, tasa, actualizado_por, editar_existente)
            else:
                st.error("Por favor, ingresa una moneda y una tasa válidas.")

# ========================
# PÁGINA: PROCESAR ARCHIVOS
# ========================
elif pagina == "📁 Procesar Archivos":
    st.header("📁 Cargar y Procesar Archivos")

    st.markdown("""
    Sube los archivos necesarios. El sistema calculará las utilidades según el tipo de tienda:
    - **Tipo A**: TODOENCARGO-CO, MEGA TIENDAS PERUANAS
    - **Tipo B**: MEGATIENDA SPA, VEENDELO  
    - **Tipo C**: DETODOPARATODOS, COMPRAFACIL, COMPRA-YA
    - **Tipo D**: FABORCARGO
    """)

    # Crear columnas para organizar las cargas
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📊 Archivos Principales")

        drapify_file = st.file_uploader(
            "DRAPIFY (Orders_XXXXXXXX)",
            type=['xlsx', 'xls'],
            key="drapify",
            help="Archivo principal con todas las órdenes de MercadoLibre")

        anican_file = st.file_uploader(
            "Anican Logistics",
            type=['xlsx', 'xls'],
            key="anican",
            help="Archivo con costos logísticos de Anican")

        aditionals_file = st.file_uploader(
            "Anican Aditionals",
            type=['xlsx', 'xls'],
            key="aditionals",
            help="Archivo con costos adicionales de Anican")

    with col2:
        st.markdown("### 🚚 Archivos Logísticos")

        cxp_file = st.file_uploader(
            "Chile Express (CXP)",
            type=['xlsx', 'xls'],
            key="cxp",
            help="Archivo de Chile Express con costos logísticos")

    # Procesar archivos cuando se cargan
    if st.button("🔄 Procesar y Calcular Utilidades", type="primary"):
        if drapify_file:
            try:
                with st.spinner("Procesando archivos..."):
                    # 1. PROCESAR ARCHIVO DRAPIFY
                    df_drapify = pd.read_excel(drapify_file)

                    # Verificar columnas requeridas
                    columnas_requeridas = [
                        'System#', 'Serial#', 'order_id', 'account_name',
                        'date_created', 'quantity', 'logistic_type',
                        'order_status_meli', 'ETIQUETA_ENVIO', 'Declare Value',
                        'net_real_amount', 'logistic_weight_lbs',
                        'refunded_date'
                    ]

                    columnas_faltantes = [
                        col for col in columnas_requeridas
                        if col not in df_drapify.columns
                    ]

                    if columnas_faltantes:
                        st.error(
                            f"❌ Faltan columnas en DRAPIFY: {', '.join(columnas_faltantes)}"
                        )
                        st.stop() # Detener ejecución si faltan columnas

                    # Limpiar y preparar datos
                    df = df_drapify.copy()
                    df['Serial#'] = df['Serial#'].astype(str) # Asegurar tipo string para Serial#
                    df['order_id'] = df['order_id'].astype(str) # Asegurar tipo string para order_id

                    # CALCULAR COLUMNA ASIGNACION
                    df['Asignacion'] = df.apply(
                        lambda row: calcular_asignacion(
                            row['account_name'], row['Serial#']),
                        axis=1)

                    # MAPEAR PAÍS Y MONEDA
                    df['pais'] = df['account_name'].map(
                        lambda x: TIENDAS_CONFIG.get(str(x), {}).get(
                            'pais', 'Desconocido'))
                    df['tipo_calculo'] = df['account_name'].map(
                        lambda x: TIENDAS_CONFIG.get(str(x), {}).get(
                            'tipo_calculo', 'A'))

                    # Mapeo país -> moneda
                    pais_moneda = {
                        'Colombia': 'COP',
                        'Perú': 'PEN',
                        'Peru': 'PEN', # Incluir "Peru" en minúsculas por si acaso
                        'Chile': 'CLP'
                    }
                    df['moneda'] = df['pais'].map(pais_moneda)

                    # CALCULAR COLUMNAS COMUNES
                    df['Costo Amazon'] = df['Declare Value'].fillna(
                        0) * df['quantity'].fillna(1)

                    # Calcular MELI USD según TRM
                    df['MELI USD'] = df.apply(
                        lambda row: (row['net_real_amount'] / st.session_state.
                                     trm_data.get(row['moneda'], 1))
                        if pd.notna(row['net_real_amount']) and row[
                            'moneda'] in st.session_state.trm_data else 0,
                        axis=1)

                    # Inicializar TODAS las columnas requeridas que se llenarán con merges o cálculos
                    df['Aditional'] = 0.0
                    df['Bodegal'] = 0.0
                    df['Socio_cuenta'] = 0.0
                    df['Impuesto por facturacion'] = 0.0
                    df['Gss Logistica'] = 0.0
                    df['Impuesto Gss'] = 0.0
                    df['Utilidad Gss'] = 0.0
                    df['Utilidad Socio'] = 0.0
                    df['Total_Anican'] = 0.0
                    df['Amt_Due_CXP'] = 0.0
                    df['Arancel_CXP'] = 0.0
                    df['IVA_CXP'] = 0.0
                    df['Costo cxp'] = 0.0  # Costo cxp = Amt. Due según especificaciones
                    df['Peso_kg'] = 0.0 # Inicializar Peso_kg

                    # 2. PROCESAR ARCHIVO ANICAN (si existe)
                    if anican_file:
                        try:
                            df_anican = pd.read_excel(anican_file)
                            required_anican_cols = [
                                'Order number', 'Reference', 'Total'
                            ] # Simplificado a solo las necesarias para el merge

                            if all(col in df_anican.columns
                                   for col in required_anican_cols):
                                # Asegurar que las columnas de unión sean string
                                df_anican['Reference'] = df_anican['Reference'].astype(str)
                                df['order_id'] = df['order_id'].astype(str)

                                df = df.merge(df_anican[[
                                    'Reference', 'Total'
                                ]].rename(columns={'Reference': 'order_id', 'Total': 'Total_Anican'}),
                                              on='order_id',
                                              how='left')
                                df['Total_Anican'] = df['Total_Anican'].fillna(0) # Rellenar NaN después del merge

                                st.success(
                                    f"✅ Anican Logistics procesado: {df['Total_Anican'].notna().sum()} matches"
                                )
                            else:
                                st.warning(
                                    "⚠️ Archivo Anican no tiene las columnas esperadas ('Order number', 'Reference', 'Total')."
                                )
                        except Exception as e:
                            st.error(f"❌ Error procesando Anican: {str(e)}")

                    # 3. PROCESAR ARCHIVO ADITIONALS (si existe)
                    if aditionals_file:
                        try:
                            df_aditionals = pd.read_excel(aditionals_file)
                            if 'Order Id' in df_aditionals.columns and 'Quantity' in df_aditionals.columns and 'UnitPrice' in df_aditionals.columns:
                                # Calcular Aditional por Order Id
                                df_aditionals['Aditional_calc'] = df_aditionals[
                                    'Quantity'].fillna(0) * df_aditionals['UnitPrice'].fillna(0)
                                aditionals_grouped = df_aditionals.groupby(
                                    'Order Id')['Aditional_calc'].sum(
                                    ).reset_index()
                                aditionals_grouped['order_id_for_merge'] = aditionals_grouped[
                                        'Order Id'].astype(str)

                                # Unir con datos principales usando 'order_id' del Drapify (que es 'Reference' en Anican y 'Order Id' en Aditionals)
                                # Asegúrate que df['order_id'] es el campo correcto para unir (lo es según la lógica anterior)
                                df = df.merge(aditionals_grouped[[
                                    'order_id_for_merge', 'Aditional_calc'
                                ]].rename(columns={'order_id_for_merge': 'order_id'}),
                                              on='order_id',
                                              how='left')
                                df['Aditional'] = df['Aditional_calc'].fillna(0) # Rellenar NaN después del merge
                                # Eliminar columna temporal de merge
                                if 'Aditional_calc' in df.columns:
                                    df = df.drop(columns=['Aditional_calc'])

                                st.success(
                                    f"✅ Aditionals procesado: {df['Aditional'].notna().sum()} matches"
                                )
                            else:
                                st.warning(
                                    "⚠️ Archivo Aditionals no tiene las columnas esperadas ('Order Id', 'Quantity', 'UnitPrice')."
                                )
                        except Exception as e:
                            st.error(
                                f"❌ Error procesando Aditionals: {str(e)}")

                    # 4. PROCESAR ARCHIVO CXP (si existe) - CON DETECCIÓN DE HOJAS MÚLTIPLES
                    if cxp_file:
                        try:
                            st.info("🔧 Procesando archivo CXP...")

                            excel_file = pd.ExcelFile(cxp_file)
                            st.info(
                                f"📋 Hojas encontradas: {excel_file.sheet_names}"
                            )

                            df_cxp = None
                            sheet_used = None
                            header_row_used = None

                            # Probar cada hoja del archivo
                            for sheet_name in excel_file.sheet_names:
                                st.info(f"🔍 Probando hoja: '{sheet_name}'")

                                # Para cada hoja, probar diferentes filas como headers
                                for header_row in [0, 1, 2, 3, 4]: # Ampliado a más filas para headers
                                    try:
                                        df_test = pd.read_excel(
                                            cxp_file,
                                            sheet_name=sheet_name,
                                            header=header_row)

                                        # Mostrar primeras columnas de esta combinación
                                        st.info(
                                            f"    Fila {header_row}: {list(df_test.columns[:5])}"
                                        )

                                        # Buscar columnas específicas con patrones más flexibles
                                        # Convertir nombres de columna a string para asegurar comparación
                                        df_test.columns = [str(col) for col in df_test.columns]
                                        
                                        has_ref = any(
                                            re.search(r'Ref\s*#', col, re.IGNORECASE)
                                            for col in df_test.columns)
                                        has_amt = any(
                                            re.search(r'Amt\.\s*Due', col, re.IGNORECASE)
                                            for col in df_test.columns)
                                        
                                        # Identificar el nombre exacto de la columna Amt. Due y Ref #
                                        exact_ref_col = next((col for col in df_test.columns if re.search(r'Ref\s*#', col, re.IGNORECASE)), None)
                                        exact_amt_col = next((col for col in df_test.columns if re.search(r'Amt\.\s*Due', col, re.IGNORECASE)), None)
                                        
                                        # Buscar Arancel e IVA de forma más flexible
                                        exact_arancel_col = next((col for col in df_test.columns if re.search(r'Arancel', col, re.IGNORECASE)), None)
                                        exact_iva_col = next((col for col in df_test.columns if re.search(r'IVA', col, re.IGNORECASE)), None)


                                        if has_ref and has_amt:
                                            df_cxp = df_test
                                            sheet_used = sheet_name
                                            header_row_used = header_row
                                            # Renombrar columnas a los nombres esperados si no son exactos
                                            if exact_ref_col and exact_ref_col != 'Ref #':
                                                df_cxp = df_cxp.rename(columns={exact_ref_col: 'Ref #'})
                                            if exact_amt_col and exact_amt_col != 'Amt. Due':
                                                df_cxp = df_cxp.rename(columns={exact_amt_col: 'Amt. Due'})
                                            if exact_arancel_col and exact_arancel_col != 'Arancel':
                                                df_cxp = df_cxp.rename(columns={exact_arancel_col: 'Arancel'})
                                            if exact_iva_col and exact_iva_col != 'IVA':
                                                df_cxp = df_cxp.rename(columns={exact_iva_col: 'IVA'})

                                            st.success(
                                                f"✅ ¡ENCONTRADO! Hoja: '{sheet_used}', Fila Header: {header_row_used}"
                                            )
                                            break # Salir del loop de headers si encontramos
                                        else:
                                            st.info(
                                                f"    ❌ Ref# encontrado: {has_ref}, Amt. Due encontrado: {has_amt}"
                                            )

                                    except Exception as e:
                                        st.info(
                                            f"    ❌ Error leyendo hoja '{sheet_name}' con fila {header_row}: {str(e)}"
                                        )
                                if df_cxp is not None: # Si encontramos en una hoja, no necesitamos probar más
                                    break

                            # Si no encontró en ninguna hoja/fila, mostrar contenido de cada hoja como ayuda para depuración
                            if df_cxp is None:
                                st.warning(
                                    "⚠️ No se encontraron las columnas 'Ref #' y 'Amt. Due' en ninguna hoja o fila. Mostrando contenido de las primeras filas de cada hoja para inspección:"
                                )

                                for sheet_name in excel_file.sheet_names:
                                    st.info(
                                        f"📄 Contenido de hoja '{sheet_name}':")
                                    try:
                                        # Mostrar primeras 3 filas sin header
                                        df_preview = pd.read_excel(
                                            cxp_file,
                                            sheet_name=sheet_name,
                                            header=None,
                                            nrows=3)
                                        st.dataframe(df_preview)
                                    except Exception as e:
                                        st.info(
                                            f"Error leyendo hoja '{sheet_name}': {str(e)}"
                                        )
                                # Detener la ejecución si no se pudo leer el archivo CXP correctamente
                                st.error("❌ No se pudo procesar el archivo CXP. Por favor, verifica su formato.")
                                st.stop()


                            # Si tenemos datos de CXP, procesarlos
                            if df_cxp is not None:
                                st.success(
                                    f"📊 Usando hoja: '{sheet_used}', fila header: {header_row_used}"
                                )
                                st.info(
                                    f"📋 Columnas finales CXP: {list(df_cxp.columns)}"
                                )

                                # Asegurar que las columnas existan antes de usarlas
                                # Ya fueron renombradas a 'Ref #' y 'Amt. Due'
                                df_cxp_clean = df_cxp[['Ref #', 'Amt. Due']].copy()

                                if 'Arancel' in df_cxp.columns:
                                    df_cxp_clean['Arancel'] = pd.to_numeric(df_cxp['Arancel'], errors='coerce').fillna(0)
                                else:
                                    df_cxp_clean['Arancel'] = 0

                                if 'IVA' in df_cxp.columns:
                                    df_cxp_clean['IVA'] = pd.to_numeric(df_cxp['IVA'], errors='coerce').fillna(0)
                                else:
                                    df_cxp_clean['IVA'] = 0


                                # Renombrar columnas para el merge
                                df_cxp_clean = df_cxp_clean.rename(
                                    columns={
                                        'Ref #': 'Asignacion_cxp',
                                        'Amt. Due': 'Amt_Due_CXP' # Renombrado aquí para evitar conflictos directos con 'Amt_Due'
                                    })

                                # Limpiar y convertir datos
                                df_cxp_clean['Amt_Due_CXP'] = pd.to_numeric(
                                    df_cxp_clean['Amt_Due_CXP'],
                                    errors='coerce').fillna(0)
                                df_cxp_clean[
                                    'Asignacion_cxp'] = df_cxp_clean[
                                        'Asignacion_cxp'].astype(
                                            str).str.strip()

                                # Filtrar registros válidos
                                df_cxp_clean = df_cxp_clean[
                                    df_cxp_clean['Asignacion_cxp'].notna()]
                                df_cxp_clean = df_cxp_clean[
                                    df_cxp_clean['Asignacion_cxp'] != '']
                                df_cxp_clean = df_cxp_clean[
                                    df_cxp_clean['Asignacion_cxp'] !=
                                    'nan']
                                
                                st.info(
                                    f"📊 Registros válidos en CXP: {len(df_cxp_clean)}"
                                )
                                if len(df_cxp_clean) > 0:
                                    st.info(
                                        f"🔗 Ejemplos de Asignacion_cxp: {df_cxp_clean['Asignacion_cxp'].head(3).tolist()}"
                                    )

                                # Unir con datos principales
                                df['Asignacion'] = df['Asignacion'].astype(str)

                                st.info(
                                    f"🔗 Ejemplos de Asignacion DRAPIFY: {df['Asignacion'].head(3).tolist()}"
                                )

                                df = df.merge(df_cxp_clean[[
                                    'Asignacion_cxp', 'Amt_Due_CXP', 'Arancel',
                                    'IVA'
                                ]],
                                              left_on='Asignacion',
                                              right_on='Asignacion_cxp',
                                              how='left')

                                # Las columnas ya están renombradas a Amt_Due_CXP, Arancel, IVA
                                # Asegurar que los valores fusionados sean numéricos y no NaN
                                df['Amt_Due_CXP'] = df['Amt_Due_CXP'].fillna(0)
                                df['Arancel_CXP'] = df['Arancel'].fillna(0) # Usar 'Arancel' del merge
                                df['IVA_CXP'] = df['IVA'].fillna(0) # Usar 'IVA' del merge
                                df['Costo cxp'] = df['Amt_Due_CXP']  # Costo cxp = Amt. Due según especificaciones

                                # Eliminar columnas temporales del merge si existen
                                for col_to_drop in ['Asignacion_cxp', 'Arancel', 'IVA']:
                                    if col_to_drop in df.columns:
                                        df = df.drop(columns=[col_to_drop])


                                matches_found = df['Amt_Due_CXP'].gt(0).sum()
                                total_amt = df['Amt_Due_CXP'].sum()

                                st.success(
                                    f"✅ CXP procesado exitosamente!")
                                st.success(
                                    f"    📊 Matches encontrados: {matches_found}"
                                )
                                st.success(
                                    f"    💰 Total Amt Due: ${total_amt:,.2f}"
                                )

                                if matches_found > 0:
                                    cxp_stats = df[
                                        df['Amt_Due_CXP'] > 0].groupby(
                                            'account_name').agg({
                                                'order_id':
                                                'count',
                                                'Amt_Due_CXP':
                                                'sum'
                                            }).round(2)
                                    cxp_stats.columns = [
                                        'Ordenes con CXP', 'Total CXP'
                                    ]
                                    st.info(
                                        "📊 Estadísticas CXP por tienda:")
                                    st.dataframe(cxp_stats)
                            else:
                                st.warning("⚠️ El archivo CXP no pudo ser procesado.")
                        except Exception as e:
                            st.error(f"❌ Error procesando CXP: {str(e)}")
                    else:
                        st.info("ℹ️ Archivo Chile Express (CXP) no cargado. Se omitirá su procesamiento.")


                    # --- CALCULAR CAMPOS ESPECÍFICOS POR TIPO DE TIENDA ---
                    # Aplicar funciones de cálculo a las columnas correspondientes
                    df['Bodegal'] = df['logistic_type'].apply(calcular_bodega)
                    df['Socio_cuenta'] = df['order_status_meli'].apply(calcular_socio_cuenta)
                    df['Impuesto por facturacion'] = df['order_status_meli'].apply(calcular_impuesto_facturacion)
                    df['Peso_kg'] = df.apply(lambda row: convertir_libras_a_kg(row['logistic_weight_lbs']) * row['quantity'], axis=1) # Multiplicar por quantity
                    df['Gss Logistica'] = df['Peso_kg'].apply(obtener_gss_logistica)
                    df['Impuesto Gss'] = df['Arancel_CXP'] + df['IVA_CXP'] # Sumar Arancel_CXP e IVA_CXP

                    # Calcular Utilidad Gss y Utilidad Socio según el tipo de cálculo
                    def calcular_utilidad_gss(row):
                        tipo = row['tipo_calculo']
                        meli_usd = row['MELI USD']
                        costo_amazon = row['Costo Amazon']
                        total_anican = row['Total_Anican']
                        aditional = row['Aditional']
                        costo_cxp = row['Costo cxp']
                        bodegal = row['Bodegal']
                        socio_cuenta = row['Socio_cuenta']
                        impuesto_facturacion = row['Impuesto por facturacion']
                        gss_logistica = row['Gss Logistica']
                        impuesto_gss = row['Impuesto Gss']
                        amt_due_cxp = row['Amt_Due_CXP']

                        if tipo == 'A':
                            return meli_usd - costo_amazon - total_anican - aditional
                        elif tipo == 'B':
                            return meli_usd - costo_cxp - costo_amazon - bodegal - socio_cuenta
                        elif tipo == 'C':
                            utilidad_gss_temp = meli_usd - costo_amazon - total_anican - aditional - impuesto_facturacion
                            # Aplicar lógica de "Utilidad Socio"
                            utilidad_socio = 7.5 if utilidad_gss_temp > 7.5 else utilidad_gss_temp
                            # Actualizar el valor de Utilidad Socio en el DataFrame directamente si fuera posible o retornar para otra columna
                            # Para fines de este cálculo, solo retornamos el Gss
                            if utilidad_gss_temp > 7.5:
                                return utilidad_gss_temp - utilidad_socio
                            else:
                                return 0
                        elif tipo == 'D':
                            return gss_logistica + impuesto_gss - amt_due_cxp # Amt_Due es el Costo cxp
                        return 0

                    # Aplicar la función de cálculo de Utilidad Gss
                    df['Utilidad Gss'] = df.apply(calcular_utilidad_gss, axis=1)

                    # Calcular Utilidad Socio para Tipo C específicamente
                    def calcular_utilidad_socio_tipo_c(row):
                        if row['tipo_calculo'] == 'C':
                            utilidad_gss_temp = row['MELI USD'] - row['Costo Amazon'] - row['Total_Anican'] - row['Aditional'] - row['Impuesto por facturacion']
                            return 7.5 if utilidad_gss_temp > 7.5 else utilidad_gss_temp
                        return 0.0 # Valor por defecto para otros tipos

                    df['Utilidad Socio'] = df.apply(calcular_utilidad_socio_tipo_c, axis=1)


                    st.session_state.processed_data = df
                    st.success("✅ Archivos procesados y utilidades calculadas con éxito!")

                    # Mostrar resumen
                    st.subheader("📊 Resumen de Datos Procesados")
                    st.dataframe(df.head()) # Mostrar las primeras filas del DataFrame procesado

                    # Mostrar estadísticas básicas por tienda
                    st.subheader("📈 Utilidad Gss por Tienda")
                    summary_utilidad = df.groupby('account_name')['Utilidad Gss'].sum().reset_index()
                    st.dataframe(summary_utilidad.style.format({"Utilidad Gss": "{:,.2f}"}))

                    # Opción para descargar el DataFrame procesado
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Descargar datos procesados (CSV)",
                        data=csv,
                        file_name=f"datos_contables_procesados_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                    )

            except Exception as e:
                st.error(f"❌ Ocurrió un error general durante el procesamiento: {str(e)}")
                st.exception(e)
        else:
            st.warning("⚠️ Por favor, carga el archivo 'DRAPIFY (Orders_XXXXXX)' para iniciar el procesamiento.")

# ========================
# PÁGINA: DASHBOARD
# ========================
elif pagina == "📊 Dashboard":
    st.header("📊 Dashboard General")
    st.info("⚠️ Esta sección está en desarrollo. ¡Pronto verás aquí los gráficos y métricas clave!")
    if st.session_state.processed_data is not None:
        st.subheader("Vista Previa de Datos Procesados")
        st.dataframe(st.session_state.processed_data.head())
        st.write(f"Total de registros procesados: {len(st.session_state.processed_data)}")
        
        # Ejemplo de métricas básicas del dashboard
        total_utilidad_gss = st.session_state.processed_data['Utilidad Gss'].sum()
        total_meli_usd = st.session_state.processed_data['MELI USD'].sum()

        col_dash1, col_dash2 = st.columns(2)
        with col_dash1:
            st.metric("Total Utilidad Gss (USD)", f"${total_utilidad_gss:,.2f}")
        with col_dash2:
            st.metric("Total MELI USD", f"${total_meli_usd:,.2f}")

# ========================
# PÁGINA: ANÁLISIS DE UTILIDAD
# ========================
elif pagina == "📈 Análisis de Utilidad":
    st.header("📈 Análisis de Utilidad")
    st.info("⚠️ Módulo en construcción para análisis avanzados por tienda. ¡Prepárate para insights detallados!")
    if st.session_state.processed_data is not None:
        st.subheader("Análisis Detallado por Tienda")
        selected_account = st.selectbox(
            "Selecciona una tienda para analizar:", 
            st.session_state.processed_data['account_name'].unique()
        )
        if selected_account:
            df_filtered = st.session_state.processed_data[st.session_state.processed_data['account_name'] == selected_account]
            
            st.write(f"Datos para **{selected_account}**:")
            st.dataframe(df_filtered[['order_id', 'MELI USD', 'Costo Amazon', 'Total_Anican', 'Aditional', 'Costo cxp', 'Bodegal', 'Socio_cuenta', 'Impuesto por facturacion', 'Gss Logistica', 'Impuesto Gss', 'Utilidad Gss']].head())

            # Sumarios por la tienda seleccionada
            st.subheader(f"Sumario de Utilidad para {selected_account}")
            total_utilidad_tienda = df_filtered['Utilidad Gss'].sum()
            st.metric("Utilidad Gss (USD) para esta tienda", f"${total_utilidad_tienda:,.2f}")

            # Puedes añadir más gráficos o tablas aquí, por ejemplo:
            # st.line_chart(df_filtered.set_index('date_created')['Utilidad Gss'])

