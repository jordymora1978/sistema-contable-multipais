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
    
    # PASO 2: Calcular Asignacion - CORREGIDO CON NOMBRES REALES
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
    """Inserta datos en Supabase con validación de duplicados Y MAPEO DE COLUMNAS"""
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
        
        # Obtener columnas disponibles en la tabla orders
        st.info("📋 Verificando estructura de la tabla...")
        try:
            # Hacer una consulta simple para obtener la estructura
            test_result = supabase.table('orders').select('*').limit(1).execute()
            available_columns = set()
            if test_result.data:
                available_columns = set(test_result.data[0].keys())
            else:
                # Si no hay datos, usar introspección básica
                sample_insert = supabase.table('orders').select('*').limit(0).execute()
        except Exception as e:
            st.warning(f"No se pudo verificar estructura: {e}")
            available_columns = set()
        
        # Preparar registros para inserción
        records = df_to_insert.to_dict('records')
        
        # MAPEO COMPLETO DE NOMBRES DE COLUMNAS: DataFrame → Supabase
        column_mapping = {
            # Columnas principales
            'ASIN': 'asin',
            'Serial#': 'serial_number',
            'System#': 'system_number',
            'Asignacion': 'asignacion',
            
            # Columnas de cantidad y producto
            'quantity': 'quantity_drapify',
            
            # Columnas financieras
            'Declare Value': 'declare_value',
            'Meli Fee': 'meli_fee',
            'IVA': 'iva',
            'ICA': 'ica',
            'FUENTE': 'fuente',
            
            # Información personal
            'Estado': 'estado',
            'Ciudad': 'ciudad',
            'Numero de documento': 'numero_de_documento',
            
            # Pesos y medidas
            'Fixed Weight': 'fixed_weight',
            'Amazon Weight': 'amazon_weight',
            'Cargo Weight': 'cargo_weight',
            
            # Etiquetas y fechas
            'ETIQUETA_ENVIO': 'etiqueta_envio',
            'LIBERATION DATE': 'liberation_date',
            'MWB': 'mwb',
            'Flight Date': 'flight_date',
            'PA Declare Value': 'pa_declare_value',
            
            # Costos de courier
            'Freight Currier': 'freight_currier',
            'Freight Currier Users': 'freight_currier_users',
            'Freight Currier2': 'freight_currier2',
            'Freight Currier2 Users': 'freight_currier2_users',
            
            # Duties y fees
            'Duties Prealert': 'duties_prealert',
            'Custom Duty Fee': 'custom_duty_fee',
            'Saving': 'saving',
            'Local Delivery Corporativo': 'local_delivery_corporativo',
            'National Shipment From': 'national_shipment_from',
            
            # Packages
            'fullfilment package': 'fullfilment_package',
            'Package Consolidated': 'package_consolidated',
            'Buy Product Fee': 'buy_product_fee',
            'Total Master': 'total_master',
            'Total User': 'total_user',
            
            # Otros
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
                
                # PASO 3: Filtrar columnas que no existen en la tabla
                if available_columns and mapped_key not in available_columns:
                    continue
                
                # PASO 4: Limpiar valores
                if pd.isna(value):
                    cleaned_record[mapped_key] = None
                elif isinstance(value, (pd.Timestamp, pd.DatetimeIndex)):
                    # Convertir fechas pandas a string
                    cleaned_record[mapped_key] = value.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(value) else None
                elif hasattr(value, 'isoformat'):
                    # Fechas datetime normales
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
                # DIAGNÓSTICO DETALLADO DEL ERROR
                error_msg = str(batch_error)
                st.error(f"Error en lote {i//batch_size + 1}: {error_msg}")
                
                # Mostrar detalles del primer registro del lote para debugging
                if i == 0:  # Solo para el primer lote
                    st.error("🔍 DEBUGGING - Primer registro del lote:")
                    st.json(batch[0])
                    st.error("🔍 DEBUGGING - Columnas disponibles en tabla:")
                    st.write(list(available_columns) if available_columns else "No se pudo obtener")
                    
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

# PÁGINAS DE LA APLICACIÓN
def mostrar_consolidador():
    """Página del consolidador de archivos - CON INSERCIÓN AUTOMÁTICA"""
    
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
            # Botón de descarga siempre disponible
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
            st.markdown("**📅 Configuración Fecha Logistics**")
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
            
            st.success(f"✅ Fecha Logistics: **{logistics_date.strftime('%Y-%m-%d')}**")
        
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
    
    # BOTÓN DE PROCESAMIENTO - Siempre habilitado si hay archivo Drapify
    button_text = "🔄 Reprocesar Archivos" if st.session_state.processing_complete else "🚀 Procesar Archivos"
    if st.button(button_text, disabled=not drapify_file, type="primary"):
        
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
                
                # GUARDAR EN SESSION STATE (sobrescribir datos anteriores)
                st.session_state.consolidated_data = consolidated_df
                st.session_state.processing_complete = True
                st.session_state.last_processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                st.session_state.processing_stats = {
                    'total_registros': len(consolidated_df),
                    'asignaciones': consolidated_df['asignacion'].notna().sum() if 'asignacion' in consolidated_df.columns else 0,
                    'con_fecha_logistics': consolidated_df['fecha_logistics'].notna().sum() if 'fecha_logistics' in consolidated_df.columns else 0,
                    'columnas': len(consolidated_df.columns)
                }
                
                # Limpiar utilidades anteriores al reprocesar
                st.session_state.utilidades_data = None
                st.session_state.utilidades_calculated = False
                
                st.success("💾 Datos guardados en memoria (datos anteriores reemplazados)")
                
                # INSERCIÓN AUTOMÁTICA EN BASE DE DATOS
                st.header("💾 Insertando en Base de Datos")
                
                with st.spinner("Validando duplicados e insertando datos..."):
                    result = insert_to_supabase_with_validation(consolidated_df)
                
                # Mostrar resultados detallados
                st.subheader("📊 Resultado de la Inserción")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("📥 Nuevos Insertados", result['inserted'], 
                             delta=f"+{result['inserted']}" if result['inserted'] > 0 else None)
                
                with col2:
                    st.metric("⚠️ Duplicados", result['duplicates'],
                             delta="Ya existían" if result['duplicates'] > 0 else None)
                
                with col3:
                    st.metric("❌ Errores", result['errors'],
                             delta="No insertados" if result['errors'] > 0 else None)
                
                with col4:
                    total_procesados = result['inserted'] + result['duplicates'] + result['errors']
                    st.metric("📋 Total Procesados", total_procesados)
                
                # Mensajes de resultado
                if result['inserted'] > 0:
                    st.success(f"✅ {result['inserted']} registros nuevos insertados correctamente")
                
                if result['duplicates'] > 0:
                    st.warning(f"⚠️ {result['duplicates']} registros ya existían en la base de datos")
                
                if result['errors'] > 0:
                    st.error(f"❌ {result['errors']} registros tuvieron errores al insertar")
                
                if result['inserted'] > 0:
                    st.balloons()
                
                # Verificar total en BD
                try:
                    total_bd_result = supabase.table('orders').select('id', count='exact').execute()
                    total_bd = total_bd_result.count
                    st.info(f"📊 Total registros en base de datos: **{total_bd:,}**")
                except Exception as e:
                    st.warning("No se pudo verificar el total en BD")
                
            except Exception as e:
                st.error(f"❌ Error procesando archivos: {str(e)}")
                st.exception(e)
    
    # MOSTRAR DATOS PROCESADOS SI EXISTEN
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
        
        # Información de columnas
        with st.expander("📋 Ver todas las columnas"):
            st.write("**Columnas disponibles:**")
            for i, col in enumerate(st.session_state.consolidated_data.columns, 1):
                st.write(f"{i}. {col}")

def mostrar_calculo_utilidades():
    """Página de cálculo de utilidades - MEJORADA"""
    st.title("💰 Cálculo de Utilidades")
    
    if not UTILIDADES_AVAILABLE:
        st.warning("⚠️ Módulo de utilidades no disponible")
        return
    
    # Verificar si hay datos consolidados
    if not st.session_state.processing_complete or st.session_state.consolidated_data is None:
        st.warning("⚠️ Primero debes consolidar archivos en la página principal")
        st.info("👈 Ve a 'Consolidador de Archivos' para procesar datos")
        return
    
    try:
        calculadora = get_calculador_utilidades()
        st.success("✅ Módulo de utilidades cargado")
        
        # Mostrar TRM actual
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("🇨🇴 Colombia", f"${calculadora.trm_actual.get('colombia', 0):,.2f}")
        with col2:
            st.metric("🇵🇪 Perú", f"${calculadora.trm_actual.get('peru', 0):,.2f}")
        with col3:
            st.metric("🇨🇱 Chile", f"${calculadora.trm_actual.get('chile', 0):,.2f}")
        
        st.markdown("---")
        
        # Información de datos disponibles
        st.info(f"📊 Datos disponibles: {len(st.session_state.consolidated_data)} registros consolidados")
        
        # Botón para calcular utilidades
        if st.button("🚀 Calcular Utilidades", type="primary", use_container_width=True):
            with st.spinner("Calculando utilidades..."):
                try:
                    # Calcular utilidades
                    utilidades_df = calculadora.calcular_utilidades_por_cuenta(st.session_state.consolidated_data)
                    
                    # Guardar en session state
                    st.session_state.utilidades_data = utilidades_df
                    st.session_state.utilidades_calculated = True
                    
                    st.success("✅ Utilidades calculadas correctamente")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Error calculando utilidades: {str(e)}")
                    st.exception(e)
        
        # Mostrar resultados si existen
        if st.session_state.utilidades_calculated and st.session_state.utilidades_data is not None:
            st.header("📊 Resultados de Utilidades")
            
            # Estadísticas generales
            utilidades_df = st.session_state.utilidades_data
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_utilidad = utilidades_df['Utilidad Gss'].sum()
                st.metric("Utilidad Total", f"${total_utilidad:,.2f}")
            
            with col2:
                registros_con_utilidad = utilidades_df['Utilidad Gss'].notna().sum()
                st.metric("Registros Calculados", registros_con_utilidad)
            
            with col3:
                utilidad_promedio = utilidades_df['Utilidad Gss'].mean()
                st.metric("Utilidad Promedio", f"${utilidad_promedio:,.2f}")
            
            with col4:
                if 'Utilidad Socio' in utilidades_df.columns:
                    total_socio = utilidades_df['Utilidad Socio'].sum()
                    st.metric("Utilidad Socio", f"${total_socio:,.2f}")
            
            # Preview de datos
            st.subheader("👀 Preview de Utilidades")
            st.dataframe(utilidades_df.head(20), use_container_width=True)
            
            # Descarga de resultados
            st.subheader("💾 Descargar Resultados")
            
            csv_buffer = io.StringIO()
            utilidades_df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            st.download_button(
                label="📥 Descargar Utilidades CSV",
                data=csv_data,
                file_name=f"utilidades_calculadas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                type="primary"
            )
            
            # Opción para guardar en BD
            if st.button("💾 Guardar en Base de Datos", type="secondary"):
                with st.spinner("Guardando en base de datos..."):
                    if calculadora.guardar_utilidades_en_bd(utilidades_df):
                        st.success("✅ Utilidades guardadas en base de datos")
                        st.balloons()
                    else:
                        st.error("❌ Error guardando en base de datos")
        
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
        
        st.info("💡 **Importante:** Cambiar la TRM afecta TODOS los cálculos futuros de utilidades.")
        
        # CONFIGURACIÓN MANUAL DE TRM
        st.subheader("⚙️ Configurar TRM Manualmente")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**🇨🇴 COLOMBIA**")
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
            st.markdown("**🇵🇪 PERÚ**")
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
            st.markdown("**🇨🇱 CHILE**")
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
    """Dashboard con visualizaciones de utilidades"""
    st.title("📊 Dashboard de Utilidades")
    
    if not st.session_state.utilidades_calculated or st.session_state.utilidades_data is None:
        st.warning("⚠️ No hay datos de utilidades calculadas")
        st.info("👈 Ve a 'Cálculo de Utilidades' para procesar datos")
        return
    
    utilidades_df = st.session_state.utilidades_data
    
    # Estadísticas generales
    st.subheader("📈 Resumen General")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_utilidad = utilidades_df['Utilidad Gss'].sum()
        st.metric("💰 Utilidad Total", f"${total_utilidad:,.2f}")
    
    with col2:
        registros_positivos = (utilidades_df['Utilidad Gss'] > 0).sum()
        st.metric("📈 Utilidades Positivas", registros_positivos)
    
    with col3:
        registros_negativos = (utilidades_df['Utilidad Gss'] < 0).sum()
        st.metric("📉 Utilidades Negativas", registros_negativos)
    
    with col4:
        utilidad_promedio = utilidades_df['Utilidad Gss'].mean()
        st.metric("📊 Promedio", f"${utilidad_promedio:,.2f}")
    
    # Análisis por cuenta
    st.subheader("🏢 Análisis por Cuenta")
    
    if 'account_name' in utilidades_df.columns:
        utilidades_por_cuenta = utilidades_df.groupby('account_name')['Utilidad Gss'].agg(['sum', 'count', 'mean']).reset_index()
        utilidades_por_cuenta.columns = ['Cuenta', 'Utilidad Total', 'Cantidad', 'Utilidad Promedio']
        utilidades_por_cuenta = utilidades_por_cuenta.sort_values('Utilidad Total', ascending=False)
        
        st.dataframe(utilidades_por_cuenta, use_container_width=True)
    
    # Top y Bottom performers
    st.subheader("🏆 Top y Bottom Performers")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🔝 Top 10 Mejores Utilidades**")
        # Buscar columna serial disponible
        serial_col = 'serial_number'
        if 'Serial#' in utilidades_df.columns:
            serial_col = 'Serial#'
        
        top_utilidades = utilidades_df.nlargest(10, 'Utilidad Gss')[[serial_col, 'account_name', 'Utilidad Gss']]
        st.dataframe(top_utilidades, use_container_width=True)
    
    with col2:
        st.markdown("**🔻 Top 10 Peores Utilidades**")
        bottom_utilidades = utilidades_df.nsmallest(10, 'Utilidad Gss')[[serial_col, 'account_name', 'Utilidad Gss']]
        st.dataframe(bottom_utilidades, use_container_width=True)

def mostrar_reportes():
    """Página de reportes"""
    st.title("📋 Reportes")
    
    if not st.session_state.processing_complete:
        st.warning("⚠️ No hay datos consolidados disponibles")
        st.info("👈 Ve a 'Consolidador de Archivos' para procesar datos")
        return
    
    st.subheader("📊 Reportes Disponibles")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.info("**📈 Reporte de Consolidación**")
        st.write(f"• Total registros: {len(st.session_state.consolidated_data)}")
        st.write(f"• Columnas: {len(st.session_state.consolidated_data.columns)}")
        
        if st.button("📥 Descargar Consolidado", key="download_consolidated"):
            csv_buffer = io.StringIO()
            st.session_state.consolidated_data.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            st.download_button(
                label="📄 Descargar CSV",
                data=csv_data,
                file_name=f"reporte_consolidado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col2:
        if st.session_state.utilidades_calculated:
            st.info("**💰 Reporte de Utilidades**")
            total_utilidad = st.session_state.utilidades_data['Utilidad Gss'].sum()
            st.write(f"• Utilidad total: ${total_utilidad:,.2f}")
            st.write(f"• Registros calculados: {len(st.session_state.utilidades_data)}")
            
            if st.button("📥 Descargar Utilidades", key="download_utilidades"):
                csv_buffer = io.StringIO()
                st.session_state.utilidades_data.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()
                
                st.download_button(
                    label="📄 Descargar CSV",
                    data=csv_data,
                    file_name=f"reporte_utilidades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        else:
            st.warning("⚠️ No hay utilidades calculadas")

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
