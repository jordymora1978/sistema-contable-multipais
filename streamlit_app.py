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

# INICIALIZAR SESSION STATE
def init_session_state():
    """Inicializa todas las variables de session_state necesarias"""
    if 'consolidated_data' not in st.session_state:
        st.session_state.consolidated_data = None
    if 'processing_complete' not in st.session_state:
        st.session_state.processing_complete = False
    if 'utilidades_data' not in st.session_state:
        st.session_state.utilidades_data = None
    if 'utilidades_calculated' not in st.session_state:
        st.session_state.utilidades_calculated = False
    if 'processing_stats' not in st.session_state:
        st.session_state.processing_stats = {}
    if 'last_processing_time' not in st.session_state:
        st.session_state.last_processing_time = None

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
    if 'account_name' in consolidated_df.columns:
        # Buscar columna serial (puede tener diferentes nombres)
        serial_col = None
        for col in ['Serial#', 'serial_number', 'serial#']:
            if col in consolidated_df.columns:
                serial_col = col
                break
        
        if serial_col:
            consolidated_df['asignacion'] = consolidated_df.apply(
                lambda row: calculate_asignacion(row['account_name'], row[serial_col]), 
                axis=1
            )
            st.success(f"✅ Asignaciones calculadas usando columna: {serial_col}")
    
    # PASO 3: Agregar fecha logistics si hay archivo
    if logistics_df is not None and logistics_date:
        consolidated_df['fecha_logistics'] = logistics_date.strftime('%Y-%m-%d')
        st.success(f"✅ Fecha logistics aplicada: {logistics_date}")
    
    st.success(f"🎉 Consolidación completada: {len(consolidated_df)} registros")
    return consolidated_df

def insert_to_supabase_with_validation(df):
    """Inserta datos en Supabase con validación de duplicados"""
    if not supabase:
        st.error("❌ No hay conexión a Supabase")
        return {'inserted': 0, 'duplicates': 0, 'errors': 0}
        
    try:
        # Obtener order_ids existentes en la BD
        st.info("🔍 Verificando duplicados...")
        existing_result = supabase.table('orders').select('order_id').execute()
        existing_order_ids = set([row['order_id'] for row in existing_result.data if row.get('order_id')])
        
        # Filtrar registros nuevos
        df_to_insert = df[~df['order_id'].isin(existing_order_ids)].copy()
        duplicates_count = len(df) - len(df_to_insert)
        
        if len(df_to_insert) == 0:
            st.warning("⚠️ Todos los registros ya existen en la base de datos")
            return {'inserted': 0, 'duplicates': duplicates_count, 'errors': 0}
        
        # Preparar registros para inserción
        records = df_to_insert.to_dict('records')
        
        # MAPEO COMPLETO DE NOMBRES DE COLUMNAS
        column_mapping = {
            'ASIN': 'asin',
            'Serial#': 'serial_number',
            'System#': 'system_number',
            'Asignacion': 'asignacion',
            'quantity': 'quantity_drapify',
            'Declare Value': 'declare_value',
            'Meli Fee': 'meli_fee',
            'IVA': 'iva',
            'ICA': 'ica',
            'FUENTE': 'fuente',
            'Estado': 'estado',
            'Ciudad': 'ciudad',
            'Numero de documento': 'numero_de_documento',
            'Fixed Weight': 'fixed_weight',
            'Amazon Weight': 'amazon_weight',
            'Cargo Weight': 'cargo_weight',
            'ETIQUETA_ENVIO': 'etiqueta_envio',
            'LIBERATION DATE': 'liberation_date',
            'MWB': 'mwb',
            'Flight Date': 'flight_date',
            'PA Declare Value': 'pa_declare_value',
            'Freight Currier': 'freight_currier',
            'Freight Currier Users': 'freight_currier_users',
            'Freight Currier2': 'freight_currier2',
            'Freight Currier2 Users': 'freight_currier2_users',
            'Duties Prealert': 'duties_prealert',
            'Custom Duty Fee': 'custom_duty_fee',
            'Saving': 'saving',
            'Local Delivery Corporativo': 'local_delivery_corporativo',
            'National Shipment From': 'national_shipment_from',
            'fullfilment package': 'fullfilment_package',
            'Package Consolidated': 'package_consolidated',
            'Buy Product Fee': 'buy_product_fee',
            'Total Master': 'total_master',
            'Total User': 'total_user',
            'COLOR': 'color',
            'TRM': 'trm'
        }
        
        # Limpiar y preparar registros CON MAPEO
        cleaned_records = []
        for record in records:
            cleaned_record = {}
            for key, value in record.items():
                # PASO 1: Mapear nombre de columna si existe mapping
                mapped_key = column_mapping.get(key, key)
                
                # PASO 2: Convertir a minúsculas si no está en el mapping
                if mapped_key == key:
                    mapped_key = key.lower().replace(' ', '_').replace('#', '_number').replace('.', '_')
                
                # PASO 3: Limpiar valores
                if pd.isna(value):
                    cleaned_record[mapped_key] = None
                elif isinstance(value, (pd.Timestamp, pd.DatetimeIndex)):
                    cleaned_record[mapped_key] = value.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(value) else None
                elif hasattr(value, 'isoformat'):
                    cleaned_record[mapped_key] = value.isoformat()
                else:
                    cleaned_record[mapped_key] = value
            
            cleaned_records.append(cleaned_record)
        
        if not cleaned_records:
            st.error("❌ No hay datos válidos para insertar después de la limpieza")
            return {'inserted': 0, 'duplicates': duplicates_count, 'errors': len(df_to_insert)}
        
        # Insertar en lotes
        batch_size = 50
        total_inserted = 0
        errors = 0
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i in range(0, len(cleaned_records), batch_size):
            batch = cleaned_records[i:i + batch_size]
            
            try:
                result = supabase.table('orders').insert(batch).execute()
                total_inserted += len(batch)
                
                progress = min(1.0, (i + batch_size) / len(cleaned_records))
                progress_bar.progress(progress)
                status_text.text(f"Insertando lote {i//batch_size + 1}... ({total_inserted}/{len(cleaned_records)})")
                
            except Exception as batch_error:
                st.error(f"Error en lote {i//batch_size + 1}: {str(batch_error)}")
                errors += len(batch)
                continue
        
        progress_bar.progress(1.0)
        status_text.empty()
        
        return {'inserted': total_inserted, 'duplicates': duplicates_count, 'errors': errors}
        
    except Exception as e:
        st.error(f"Error general insertando: {str(e)}")
        return {'inserted': 0, 'duplicates': 0, 'errors': len(df)}

def verificar_conexion_supabase():
    if not supabase:
        return False, "No conexión"
    try:
        result = supabase.table('orders').select('id').limit(1).execute()
        return True, "Conexión exitosa"
    except Exception as e:
        return False, str(e)

def clear_session_data():
    """Limpia todos los datos de la sesión"""
    st.session_state.consolidated_data = None
    st.session_state.processing_complete = False
    st.session_state.utilidades_data = None
    st.session_state.utilidades_calculated = False
    st.session_state.processing_stats = {}
    st.session_state.last_processing_time = None

# PÁGINA PRINCIPAL
def mostrar_consolidador():
    """Página del consolidador de archivos"""
    
    # Mostrar estado de datos existentes si los hay
    if st.session_state.processing_complete and st.session_state.consolidated_data is not None:
        st.success(f"✅ Datos ya procesados: {len(st.session_state.consolidated_data)} registros")
        st.info(f"🕒 Procesado el: {st.session_state.last_processing_time}")
        
        col_clear, col_export = st.columns([1, 2])
        
        with col_clear:
            if st.button("🗑️ Limpiar Datos", type="secondary"):
                clear_session_data()
                st.rerun()
        
        with col_export:
            csv_buffer = io.StringIO()
            st.session_state.consolidated_data.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            st.download_button(
                label="📥 Descargar CSV Consolidado",
                data=csv_data,
                file_name=f"consolidated_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                type="primary"
            )
        
        st.info("💡 Puedes cargar nuevos archivos abajo para reemplazar estos datos")
        st.markdown("---")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📁 Subir Archivos")
        
        drapify_file = st.file_uploader(
            "1. Archivo Drapify (OBLIGATORIO)",
            type=['xlsx', 'xls', 'csv'],
            key="drapify",
            help="Archivo base con todas las órdenes"
        )
        
        logistics_file = st.file_uploader(
            "2. Archivo Logistics (opcional)",
            type=['xlsx', 'xls', 'csv'],
            key="logistics",
            help="Costos de Anicam para envíos internacionales"
        )
        
        logistics_date = None
        if logistics_file:
            st.markdown("---")
            st.markdown("**📅 Configuración Fecha Logistics**")
            
            col_date1, col_date2, col_date3 = st.columns([2, 1, 1])
            
            with col_date1:
                logistics_date = st.date_input(
                    "Fecha para datos de Logistics:",
                    value=datetime.now().date()
                )
            
            with col_date2:
                if st.button("📅 Usar Hoy", key="use_today"):
                    logistics_date = datetime.now().date()
                    st.rerun()
            
            with col_date3:
                if st.button("📅 Ayer", key="use_yesterday"):
                    logistics_date = datetime.now().date() - timedelta(days=1)
                    st.rerun()
            
            st.success(f"✅ Fecha Logistics: **{logistics_date.strftime('%Y-%m-%d')}**")
        
        aditionals_file = st.file_uploader(
            "3. Archivo Aditionals (opcional)",
            type=['xlsx', 'xls', 'csv'],
            key="aditionals",
            help="Costos adicionales de Anicam"
        )
        
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
    button_text = "🔄 Reprocesar Archivos" if st.session_state.processing_complete else "🚀 Procesar Archivos"
    if st.button(button_text, disabled=not drapify_file, type="primary"):
        
        with st.spinner("Procesando archivos..."):
            try:
                # Validar archivo Drapify
                if not drapify_file:
                    st.error("❌ No se encontró archivo Drapify")
                    return
                
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
                
                # GUARDAR EN SESSION STATE
                st.session_state.consolidated_data = consolidated_df
                st.session_state.processing_complete = True
                st.session_state.last_processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                st.session_state.processing_stats = {
                    'total_registros': len(consolidated_df),
                    'asignaciones': consolidated_df['asignacion'].notna().sum() if 'asignacion' in consolidated_df.columns else 0,
                    'con_fecha_logistics': consolidated_df['fecha_logistics'].notna().sum() if 'fecha_logistics' in consolidated_df.columns else 0,
                    'columnas': len(consolidated_df.columns)
                }
                
                # Limpiar utilidades anteriores
                st.session_state.utilidades_data = None
                st.session_state.utilidades_calculated = False
                
                st.success("💾 Datos guardados en memoria")
                
                # INSERCIÓN AUTOMÁTICA EN BASE DE DATOS
                st.header("💾 Insertando en Base de Datos")
                
                with st.spinner("Validando duplicados e insertando datos..."):
                    result = insert_to_supabase_with_validation(consolidated_df)
                
                # Mostrar resultados
                st.subheader("📊 Resultado de la Inserción")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("📥 Nuevos Insertados", result['inserted'])
                
                with col2:
                    st.metric("⚠️ Duplicados", result['duplicates'])
                
                with col3:
                    st.metric("❌ Errores", result['errors'])
                
                with col4:
                    total_procesados = result['inserted'] + result['duplicates'] + result['errors']
                    st.metric("📋 Total Procesados", total_procesados)
                
                # Mensajes de resultado
                if result['inserted'] > 0:
                    st.success(f"✅ {result['inserted']} registros nuevos insertados")
                    st.balloons()
                
                if result['duplicates'] > 0:
                    st.warning(f"⚠️ {result['duplicates']} registros ya existían")
                
                if result['errors'] > 0:
                    st.error(f"❌ {result['errors']} registros con errores")
                
                # Verificar total en BD
                try:
                    total_bd_result = supabase.table('orders').select('id', count='exact').execute()
                    total_bd = total_bd_result.count
                    st.info(f"📊 Total en BD: **{total_bd:,}**")
                except Exception as e:
                    st.warning("No se pudo verificar el total en BD")
                
            except Exception as e:
                st.error(f"❌ Error procesando archivos: {str(e)}")
                st.exception(e)
    
    # MOSTRAR DATOS PROCESADOS
    if st.session_state.processing_complete and st.session_state.consolidated_data is not None:
        st.header("📊 Datos Consolidados")
        
        # Estadísticas
        col1, col2, col3, col4 = st.columns(4)
        
        stats = st.session_state.processing_stats
        
        with col1:
            st.metric("Total Registros", stats.get('total_registros', 0))
        
        with col2:
            st.metric("Asignaciones", stats.get('asignaciones', 0))
        
        with col3:
            st.metric("Con Fecha Logistics", stats.get('con_fecha_logistics', 0))
        
        with col4:
            st.metric("Columnas", stats.get('columnas', 0))
        
        # Preview de datos
        st.header("👀 Preview de Datos")
        st.dataframe(st.session_state.consolidated_data.head(20), use_container_width=True)

# OTRAS PÁGINAS (PLACEHOLDER)
def mostrar_calculo_utilidades():
    st.title("💰 Cálculo de Utilidades")
    if not UTILIDADES_AVAILABLE:
        st.warning("⚠️ Módulo de utilidades no disponible")
        return
    st.info("Funcionalidad disponible cuando hay datos consolidados")

def mostrar_gestion_trm():
    st.title("💱 Gestión de TRM")
    st.info("Gestión de tasas de cambio")

def mostrar_dashboard_utilidades():
    st.title("📊 Dashboard de Utilidades")
    st.info("Dashboard con visualizaciones")

def mostrar_reportes():
    st.title("📋 Reportes")
    st.info("Generación de reportes")

# FUNCIÓN PRINCIPAL
def main():
    # Inicializar session state
    init_session_state()
    
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
        
        # Estado de la sesión
        if st.session_state.processing_complete:
            st.success(f"📊 Datos: {len(st.session_state.consolidated_data)} registros")
        
        if st.session_state.utilidades_calculated:
            total_utilidad = st.session_state.utilidades_data['Utilidad Gss'].sum()
            st.info(f"💰 Utilidades: ${total_utilidad:,.0f}")
        
        st.markdown("---")
        
        # NAVEGACIÓN
        pagina = st.selectbox("📋 Navegación", [
            "🏠 Consolidador de Archivos",
            "💰 Cálculo de Utilidades",
            "💱 Gestión TRM",
            "📊 Dashboard Utilidades", 
            "📋 Reportes"
        ])
        
        st.markdown("---")
        st.info("🎯 Modo: Procesar e insertar automáticamente en BD")
        
        # Botón para limpiar toda la sesión
        if st.session_state.processing_complete or st.session_state.utilidades_calculated:
            if st.button("🗑️ Limpiar Todo", type="secondary"):
                clear_session_data()
                st.rerun()
    
    # ROUTING
    if pagina == "🏠 Consolidador de Archivos":
        mostrar_consolidador()
    elif pagina == "💰 Cálculo de Utilidades":
        mostrar_calculo_utilidades()
    elif pagina == "💱 Gestión TRM":
        mostrar_gestion_trm()
    elif pagina == "📊 Dashboard Utilidades":
        mostrar_dashboard_utilidades()
    elif pagina == "📋 Reportes":
        mostrar_reportes()

if __name__ == "__main__":
    main()
