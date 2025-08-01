import streamlit as st
import pandas as pd
import numpy as np
from supabase import create_client, Client
import os
from datetime import datetime, timedelta
import io
import time

# IMPORTS PARA UTILIDADES (solo si el módulo existe)
try:
from modulo_utilidades import get_calculador_utilidades
import plotly.express as px
import plotly.graph_objects as go
UTILIDADES_AVAILABLE = True
except ImportError:
UTILIDADES_AVAILABLE = False

# Configuración de la página
st.set_page_config(
page_title="Sistema de Gestión Integral",
page_icon="💰",
layout="wide",
initial_sidebar_state="expanded"
)

# Configuración de Supabase
@st.cache_resource
def init_supabase():
url = "https://qzexuqkedukcwcyhrpza.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"
return create_client(url, key)

supabase = init_supabase()

# FUNCIONES UTILITARIAS
def clean_id(value):
"""Limpia y normaliza IDs removiendo comillas y espacios"""
if pd.isna(value):
return None
str_value = str(value).strip()
if str_value.startswith("'"):
str_value = str_value[1:]
if str_value.endswith('.0'):
str_value = str_value[:-2]
return str_value if str_value and str_value != 'nan' else None

def format_currency_no_decimals(value):
"""Formatea números como currency sin decimales"""
if pd.isna(value):
return None
try:
num_value = float(value)
return f"${num_value:,.0f}"
except (ValueError, TypeError):
return value

def apply_formatting(df):
"""Aplica formateos básicos al DataFrame"""
st.info("🎨 Aplicando formateos...")

# Formato Currency básico
currency_columns = ['unit_price', 'net_real_amount', 'declare_value']

for col in currency_columns:
if col in df.columns:
df[col] = df[col].apply(format_currency_no_decimals)

st.success("🎨 Formateos aplicados correctamente")
return df

def calculate_asignacion(account_name, serial_number):
"""Calcula la asignación basada en el account_name y serial_number"""
if pd.isna(account_name) or pd.isna(serial_number):
return None

clean_serial = clean_id(serial_number)
if not clean_serial:
return None

account_mapping = {
'1-TODOENCARGO-CO': 'TDC',
'2-MEGATIENDA SPA': 'MEGA',
'4-MEGA TIENDAS PERUANAS': 'MGA-PE',
'5-DETODOPARATODOS': 'DTPT',
'6-COMPRAFACIL': 'CFA',
'7-COMPRA-YA': 'CPYA',
'8-FABORCARGO': 'FBC',
'3-VEENDELO': 'VEEN'
}

prefix = account_mapping.get(account_name, '')
return f"{prefix}{clean_serial}" if prefix else clean_serial

def process_files_according_to_rules(drapify_df, logistics_df=None, aditionals_df=None, cxp_df=None, logistics_date=None):
"""Procesa y consolida archivos según las reglas especificadas"""

st.info("🔄 Iniciando consolidación...")

# PASO 1: Usar Drapify como base
consolidated_df = drapify_df.copy()
st.success(f"✅ Drapify procesado: {len(consolidated_df)} registros")

# PASO 2: Calcular Asignacion
if 'account_name' in consolidated_df.columns and 'Serial#' in consolidated_df.columns:
consolidated_df['Asignacion'] = consolidated_df.apply(
lambda row: calculate_asignacion(row['account_name'], row['Serial#']),
axis=1
)
st.success(f"✅ Asignaciones calculadas")

# PASO 3: Agregar fecha logistics si hay archivo
if logistics_df is not None and logistics_date:
consolidated_df['fecha_logistics'] = logistics_date.strftime('%Y-%m-%d')
st.success(f"✅ Fecha logistics aplicada: {logistics_date}")

st.success(f"🎉 Consolidación completada: {len(consolidated_df)} registros")
return consolidated_df

def insert_to_supabase(df):
"""Inserta datos en Supabase"""
if not supabase:
st.error("❌ No hay conexión a Supabase")
return 0

try:
records = df.to_dict('records')

for record in records:
for key, value in record.items():
if pd.isna(value):
record[key] = None

batch_size = 50
total_inserted = 0

progress_bar = st.progress(0)

for i in range(0, len(records), batch_size):
batch = records[i:i + batch_size]

try:
result = supabase.table('orders').insert(batch).execute()
total_inserted += len(batch)

progress = min(1.0, (i + batch_size) / len(records))
progress_bar.progress(progress)

except Exception as batch_error:
st.error(f"Error en lote: {str(batch_error)}")
continue

progress_bar.progress(1.0)
return total_inserted

except Exception as e:
st.error(f"Error general: {str(e)}")
return 0

def verificar_conexion_supabase():
if not supabase:
return False, "No conexión"
try:
result = supabase.table('orders').select('id').limit(1).execute()
return True, "Conexión exitosa"
except Exception as e:
return False, str(e)

# PÁGINAS DE LA APLICACIÓN
def mostrar_consolidador(processing_mode):
"""Página del consolidador de archivos - COMPLETA"""

col1, col2 = st.columns([2, 1])

with col1:
st.header("📁 Subir Archivos")

# ARCHIVO DRAPIFY (OBLIGATORIO)
drapify_file = st.file_uploader(
"1. Archivo Drapify (OBLIGATORIO - Base de datos)",
type=['xlsx', 'xls', 'csv'],
key="drapify",
help="Archivo base con todas las órdenes"
)

# ARCHIVO LOGISTICS (OPCIONAL)
logistics_file = st.file_uploader(
"2. Archivo Logistics (opcional)",
type=['xlsx', 'xls', 'csv'],
key="logistics",
help="Costos de Anicam para envíos internacionales"
)

# FECHA MANUAL PARA LOGISTICS
logistics_date = None
if logistics_file:
st.markdown("---")
st.markdown("📅 Configuración Fecha Logistics")
st.info("💡 Esta fecha se usará para todos los registros de Logistics")

col_date1, col_date2, col_date3 = st.columns([2, 1, 1])

with col_date1:
logistics_date = st.date_input(
"Fecha para datos de Logistics:",
value=datetime.now().date(),
help="Fecha que representa cuándo se cerraron estos costos"
)

with col_date2:
if st.button("📅 Usar Hoy", key="use_today"):
logistics_date = datetime.now().date()
st.rerun()

with col_date3:
if st.button("📅 Ayer", key="use_yesterday"):
logistics_date = datetime.now().date() - timedelta(days=1)
st.rerun()

st.success(f"✅ Fecha Logistics: {logistics_date.strftime('%Y-%m-%d')}")

# ARCHIVO ADITIONALS (OPCIONAL)
aditionals_file = st.file_uploader(
"3. Archivo Aditionals (opcional)",
type=['xlsx', 'xls', 'csv'],
key="aditionals",
help="Costos adicionales de Anicam"
)

# ARCHIVO CXP (OPCIONAL)
cxp_file = st.file_uploader(
"4. Archivo CXP (opcional)",
type=['xlsx', 'xls', 'csv'],
key="cxp",
help="Costos de Chilexpress"
)

with col2:
st.header("📊 Estado")

files_status = {
"Drapify": "✅" if drapify_file else "❌",
"Logistics": "✅" if logistics_file else "⚪",
"Aditionals": "✅" if aditionals_file else "⚪",
"CXP": "✅" if cxp_file else "⚪"
}

for file_type, status in files_status.items():
st.write(f"{status} {file_type}")

if logistics_file and logistics_date:
st.markdown("---")
st.write(f"🗓️ Fecha Logistics: {logistics_date}")

st.markdown("---")

if drapify_file:
st.success("✅ Listo para procesar")
else:
st.warning("⚠️ Archivo Drapify requerido")

# BOTÓN DE PROCESAMIENTO
if st.button("🚀 Procesar Archivos", disabled=not drapify_file, type="primary"):

with st.spinner("Procesando archivos..."):
try:
# Leer archivo Drapify
if drapify_file.name.endswith('.csv'):
drapify_df = pd.read_csv(drapify_file)
else:
drapify_df = pd.read_excel(drapify_file)

st.success(f"✅ Drapify cargado: {len(drapify_df)} registros")

# Leer archivos opcionales
logistics_df = None
if logistics_file:
if logistics_file.name.endswith('.csv'):
logistics_df = pd.read_csv(logistics_file)
else:
logistics_df = pd.read_excel(logistics_file)
st.success(f"✅ Logistics cargado: {len(logistics_df)} registros")

aditionals_df = None
if aditionals_file:
if aditionals_file.name.endswith('.csv'):
aditionals_df = pd.read_csv(aditionals_file)
else:
aditionals_df = pd.read_excel(aditionals_file)
st.success(f"✅ Aditionals cargado: {len(aditionals_df)} registros")

cxp_df = None
if cxp_file:
if cxp_file.name.endswith('.csv'):
cxp_df = pd.read_csv(cxp_file)
else:
cxp_df = pd.read_excel(cxp_file)
st.success(f"✅ CXP cargado: {len(cxp_df)} registros")

# Procesar consolidación
consolidated_df = process_files_according_to_rules(
drapify_df, logistics_df, aditionals_df, cxp_df, logistics_date
)

# Aplicar formateos
st.header("🎨 Aplicando Formateos")
consolidated_df = apply_formatting(consolidated_df)

# Mostrar preview
st.header("👀 Preview de Datos Consolidados")
st.dataframe(consolidated_df.head(10), use_container_width=True)

# Estadísticas
col1, col2, col3, col4 = st.columns(4)

with col1:
st.metric("Total Registros", len(consolidated_df))

with col2:
asignaciones = consolidated_df['Asignacion'].notna().sum() if 'Asignacion' in consolidated_df.columns else 0
st.metric("Asignaciones", asignaciones)

with col3:
con_fecha = consolidated_df['fecha_logistics'].notna().sum() if 'fecha_logistics' in consolidated_df.columns else 0
st.metric("Con Fecha Logistics", con_fecha)

with col4:
st.metric("Columnas", len(consolidated_df.columns))

# Opción de descarga
st.header("💾 Descargar Resultado")

csv_buffer = io.StringIO()
consolidated_df.to_csv(csv_buffer, index=False)
csv_data = csv_buffer.getvalue()

st.download_button(
label="📥 Descargar CSV Consolidado",
data=csv_data,
file_name=f"consolidated_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
mime="text/csv",
type="primary"
)

# Insertar en BD si se seleccionó
if processing_mode == "Consolidar e insertar en DB":
st.header("💾 Insertar en Base de Datos")

if st.button("🚀 Insertar en Supabase", type="secondary"):
with st.spinner("Insertando datos..."):
inserted_count = insert_to_supabase(consolidated_df)

if inserted_count > 0:
st.success(f"✅ {inserted_count} registros insertados!")
st.balloons()
else:
st.error("❌ Error insertando datos")

except Exception as e:
st.error(f"❌ Error procesando archivos: {str(e)}")
st.exception(e)

def mostrar_calculo_utilidades():
st.title("💰 Cálculo de Utilidades")
if not UTILIDADES_AVAILABLE:
st.warning("⚠️ Módulo de utilidades no disponible")
return
try:
calculador = get_calculador_utilidades()
st.success("✅ Módulo de utilidades cargado")
col1, col2, col3 = st.columns(3)
with col1:
st.metric("🇨🇴 Colombia", f"${calculador.trm_actual.get('colombia', 0):,.2f}")
with col2:
st.metric("🇵🇪 Perú", f"${calculador.trm_actual.get('peru', 0):,.2f}")
with col3:
st.metric("🇨🇱 Chile", f"${calculador.trm_actual.get('chile', 0):,.2f}")
except Exception as e:
st.error(f"❌ Error: {str(e)}")

def mostrar_gestion_trm():
"""GESTIÓN MANUAL DE TRM"""
st.title("💱 Gestión de TRM")
st.markdown("### Control Manual de Tasas Representativas del Mercado")

if not UTILIDADES_AVAILABLE:
st.warning("⚠️ Módulo de utilidades no disponible")
return

try:
calculador = get_calculador_utilidades()

st.info("💡 Importante: Cambiar la TRM afecta TODOS los cálculos futuros de utilidades.")

# CONFIGURACIÓN MANUAL DE TRM
st.subheader("⚙️ Configurar TRM Manualmente")

col1, col2, col3 = st.columns(3)

with col1:
st.markdown("🇨🇴 COLOMBIA")
nueva_trm_colombia = st.number_input(
"TRM Colombia (COP/USD):",
value=float(calculador.trm_actual.get('colombia', 4250.0)),
min_value=1000.0,
max_value=10000.0,
step=0.01,
format="%.2f"
)
st.caption(f"Actual: ${calculador.trm_actual.get('colombia', 0):,.2f}")

with col2:
st.markdown("🇵🇪 PERÚ")
nueva_trm_peru = st.number_input(
"TRM Perú (PEN/USD):",
value=float(calculador.trm_actual.get('peru', 3.75)),
min_value=1.0,
max_value=10.0,
step=0.01,
format="%.2f"
)
st.caption(f"Actual: ${calculador.trm_actual.get('peru', 0):,.2f}")

with col3:
st.markdown("🇨🇱 CHILE")
nueva_trm_chile = st.number_input(
"TRM Chile (CLP/USD):",
value=float(calculador.trm_actual.get('chile', 850.0)),
min_value=500.0,
max_value=1500.0,
step=0.01,
format="%.2f"
)
st.caption(f"Actual: ${calculador.trm_actual.get('chile', 0):,.2f}")

# BOTÓN PARA ACTUALIZAR
st.markdown("---")
if st.button("💾 ACTUALIZAR TRM", type="primary", use_container_width=True):
nuevas_trm = {
'colombia': nueva_trm_colombia,
'peru': nueva_trm_peru,
'chile': nueva_trm_chile
}

with st.spinner("Actualizando TRM..."):
if calculador.actualizar_trm(nuevas_trm, "usuario_manual"):
st.success("✅ ¡TRM actualizada exitosamente!")
st.balloons()
st.rerun()

# HISTORIAL
st.subheader("📋 Últimos Cambios")
try:
result = supabase.table('trm_history').select('*').order('fecha_cambio', desc=True).limit(5).execute()
if result.data:
historial_df = pd.DataFrame(result.data)
st.dataframe(historial_df, use_container_width=True)
else:
st.info("No hay cambios registrados")
except Exception as e:
st.error(f"Error: {str(e)}")

except Exception as e:
st.error(f"❌ Error: {str(e)}")

def mostrar_dashboard_utilidades():
st.title("📊 Dashboard de Utilidades")
st.info("📝 No hay datos de utilidades para mostrar")

def mostrar_reportes():
st.title("📋 Reportes")
st.info("🚧 Funcionalidad en desarrollo")

# FUNCIÓN PRINCIPAL
def main():
st.title("💰 Sistema de Gestión Integral")
st.markdown("### Consolidación de archivos y cálculo de utilidades")

conexion_ok, mensaje_conexion = verificar_conexion_supabase()

# Sidebar
with st.sidebar:
st.image("https://via.placeholder.com/150x50/4F46E5/white?text=LOGO", width=150)
st.markdown("---")

if conexion_ok:
st.success("✅ Supabase conectado")
else:
st.error("❌ Sin conexión BD")

st.markdown("---")

# NAVEGACIÓN SIEMPRE CON 5 OPCIONES
pagina = st.selectbox("📋 Navegación", [
"🏠 Consolidador de Archivos",
"💰 Cálculo de Utilidades",
"💱 Gestión TRM",
"📊 Dashboard Utilidades",
"📋 Reportes"
])

st.markdown("---")
processing_mode = st.radio("Modo:", ["Solo consolidar", "Consolidar e insertar en DB"])

# ROUTING
if pagina == "🏠 Consolidador de Archivos":
mostrar_consolidador(processing_mode)
elif pagina == "💰 Cálculo de Utilidades":
mostrar_calculo_utilidades()
elif pagina == "💱 Gestión TRM":
mostrar_gestion_trm()
elif pagina == "📊 Dashboard Utilidades":
mostrar_dashboard_utilidades()
elif pagina == "📋 Reportes":
mostrar_reportes()

if name == "main":
main()
