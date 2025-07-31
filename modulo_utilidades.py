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
    # Credenciales actualizadas correctas
    url = "https://qzexuqkedukcwcyhrpza.supabase.co"
    key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"
    return create_client(url, key)

supabase = init_supabase()

# ===============================================
# FUNCIONES UTILITARIAS
# ===============================================

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

def fix_accents(text):
    """Corrige automáticamente todos los caracteres con encoding incorrecto UTF-8"""
    if pd.isna(text) or not isinstance(text, str):
        return text
    
    try:
        result = text.encode('latin-1').decode('utf-8')
        return result
    except (UnicodeDecodeError, UnicodeEncodeError):
        replacements = {
            'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú',
            'Ã±': 'ñ', 'Ã': 'Á', 'Ã‰': 'É', 'Ã"': 'Ó', 'Ãš': 'Ú'
        }
        
        result = str(text)
        for wrong, correct in replacements.items():
            result = result.replace(wrong, correct)
        
        return result

def format_date_to_standard(date_value, input_format='auto'):
    """Convierte fechas a formato YYYY-MM-DD"""
    if pd.isna(date_value):
        return None
    
    date_str = str(date_value).strip()
    
    if not date_str or date_str == 'nan':
        return None
    
    try:
        if date_str.replace('.', '').isdigit():
            excel_date = float(date_str)
            excel_epoch = datetime(1900, 1, 1)
            actual_date = excel_epoch + timedelta(days=excel_date - 2)
            return actual_date.strftime('%Y-%m-%d')
        
        if ' ' in date_str and len(date_str) >= 16:
            date_part = date_str.split(' ')[0]
            if len(date_part) == 10 and date_part.count('-') == 2:
                return date_part
        
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                month, day, year = parts
                return f"{year.zfill(4)}-{month.zfill(2)}-{day.zfill(2)}"
        
        if len(date_str) == 10 and date_str.count('-') == 2:
            return date_str
            
        return date_str
        
    except Exception:
        return date_str

def format_currency_no_decimals(value):
    """Formatea números como currency sin decimales: $#,##0"""
    if pd.isna(value):
        return None
    try:
        num_value = float(value)
        return f"${num_value:,.0f}"
    except (ValueError, TypeError):
        return value

def format_currency_with_decimals(value):
    """Formatea números como currency con decimales: $#,##0.00"""
    if pd.isna(value):
        return None
    try:
        num_value = float(value)
        return f"${num_value:,.2f}"
    except (ValueError, TypeError):
        return value

def remove_duplicates_by_order_id(df):
    """Elimina filas duplicadas basándose en order_id"""
    if 'order_id' not in df.columns:
        return df
    
    initial_count = len(df)
    df_cleaned = df.drop_duplicates(subset=['order_id'], keep='first')
    duplicates_removed = initial_count - len(df_cleaned)
    
    if duplicates_removed > 0:
        st.warning(f"⚠️ Se eliminaron {duplicates_removed} filas duplicadas basándose en order_id")
    else:
        st.success(f"✅ No se encontraron duplicados por order_id")
    
    return df_cleaned

def apply_formatting(df):
    """Aplica todos los formateos especificados al DataFrame"""
    st.info("🎨 Aplicando formateos...")
    
    # Formato Currency sin decimales
    currency_no_decimals_columns = [
        'unit_price', 'Meli Fee', 'IVA', 'ICA', 'FUENTE', 
        'senders_cost', 'gross_amount', 'net_received_amount', 'net_real_amount'
    ]
    
    for col in currency_no_decimals_columns:
        if col in df.columns:
            df[col] = df[col].apply(format_currency_no_decimals)
    
    # Formato currency con decimales
    currency_with_decimals_columns = [
        'profit_price', 'Declare Value', 'data_base_price',
        'logistics_fob', 'logistics_weight', 'logistics_total',
        'cxp_co_aereo', 'cxp_arancel', 'cxp_iva', 'cxp_amt_due'
    ]
    
    for col in currency_with_decimals_columns:
        if col in df.columns:
            df[col] = df[col].apply(format_currency_with_decimals)
    
    # Corregir acentos
    text_columns = df.select_dtypes(include=['object']).columns
    formatted_columns = currency_no_decimals_columns + currency_with_decimals_columns
    
    for col in text_columns:
        if col not in formatted_columns:
            df[col] = df[col].apply(fix_accents)
    
    # Formatear fechas
    date_columns = ['date_created', 'cxp_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: format_date_to_standard(x))
    
    # Eliminar duplicados
    df = remove_duplicates_by_order_id(df)
    
    st.success("🎨 Todos los formateos aplicados correctamente")
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
    """Procesa y consolida todos los archivos según las reglas especificadas"""
    
    st.info("🔄 Iniciando consolidación según reglas especificadas...")
    
    # PASO 1: Usar Drapify como base
    consolidated_df = drapify_df.copy()
    st.success(f"✅ Archivo base Drapify procesado: {len(consolidated_df)} registros")
    
    # PASO 2: Procesar archivo Logistics
    if logistics_df is not None and not logistics_df.empty:
        st.info("🚚 Procesando archivo Logistics...")
        
        if logistics_date:
            st.info(f"📅 Fecha asignada a Logistics: **{logistics_date}**")
        
        # Crear diccionarios para mapeo
        logistics_dict_by_reference = {}
        logistics_dict_by_order_number = {}
        
        for idx, row in logistics_df.iterrows():
            reference = clean_id(row.get('Reference', ''))
            order_number = clean_id(row.get('Order number', ''))
            
            if reference:
                logistics_dict_by_reference[reference] = row
            if order_number:
                logistics_dict_by_order_number[order_number] = row
        
        # Inicializar columnas de Logistics
        logistics_columns = ['Guide Number', 'Order number', 'Reference', 'Status', 'FOB', 'Total']
        
        for col in logistics_columns:
            if col in logistics_df.columns:
                consolidated_df[f'logistics_{col.lower().replace(" ", "_")}'] = np.nan
        
        consolidated_df['fecha_logistics'] = None
        
        matched_by_order_id = 0
        matched_by_prealert_id = 0
        
        # Hacer matching
        for idx, row in consolidated_df.iterrows():
            order_id = clean_id(row.get('order_id', ''))
            prealert_id = clean_id(row.get('prealert_id', ''))
            
            logistics_row = None
            
            if order_id and order_id in logistics_dict_by_reference:
                logistics_row = logistics_dict_by_reference[order_id]
                matched_by_order_id += 1
            elif prealert_id and prealert_id in logistics_dict_by_order_number:
                logistics_row = logistics_dict_by_order_number[prealert_id]
                matched_by_prealert_id += 1
            
            if logistics_row is not None:
                for col in logistics_columns:
                    if col in logistics_df.columns:
                        consolidated_df.loc[idx, f'logistics_{col.lower().replace(" ", "_")}'] = logistics_row.get(col)
                
                if logistics_date:
                    consolidated_df.loc[idx, 'fecha_logistics'] = logistics_date.strftime('%Y-%m-%d')
        
        st.success(f"✅ Logistics procesado: {matched_by_order_id} matches por order_id, {matched_by_prealert_id} matches por prealert_id")
    else:
        consolidated_df['fecha_logistics'] = None
    
    # PASO 3: Procesar Aditionals
    if aditionals_df is not None and not aditionals_df.empty:
        st.info("➕ Procesando archivo Aditionals...")
        
        aditionals_dict = {}
        for idx, row in aditionals_df.iterrows():
            order_id = clean_id(row.get('Order Id', ''))
            if order_id:
                aditionals_dict[order_id] = row
        
        aditionals_columns = ['Order Id', 'Total']
        for col in aditionals_columns:
            if col in aditionals_df.columns:
                consolidated_df[f'aditionals_{col.lower().replace(" ", "_")}'] = np.nan
        
        matched_aditionals = 0
        for idx, row in consolidated_df.iterrows():
            prealert_id = clean_id(row.get('prealert_id', ''))
            
            if prealert_id and prealert_id in aditionals_dict:
                aditionals_row = aditionals_dict[prealert_id]
                matched_aditionals += 1
                
                for col in aditionals_columns:
                    if col in aditionals_df.columns:
                        consolidated_df.loc[idx, f'aditionals_{col.lower().replace(" ", "_")}'] = aditionals_row.get(col)
        
        st.success(f"✅ Aditionals procesado: {matched_aditionals} matches por prealert_id")
    
    # PASO 4: Calcular Asignacion
    st.info("🏷️ Calculando columna Asignacion...")
    
    if 'account_name' in consolidated_df.columns and 'Serial#' in consolidated_df.columns:
        consolidated_df['Asignacion'] = consolidated_df.apply(
            lambda row: calculate_asignacion(row['account_name'], row['Serial#']), 
            axis=1
        )
        asignaciones_calculadas = consolidated_df['Asignacion'].notna().sum()
        st.success(f"✅ Asignaciones calculadas: {asignaciones_calculadas}")
    
    # PASO 5: Procesar CXP
    if cxp_df is not None and not cxp_df.empty:
        st.info("💰 Procesando archivo CXP...")
        
        # Normalizar columnas CXP
        column_mapping = {
            'OT Number': 'OT Number',
            'Date': 'Date', 
            'Ref #': 'Ref #',
            'Amt. Due': 'Amt. Due'
        }
        
        cxp_df_normalized = cxp_df.rename(columns=column_mapping)
        
        cxp_dict = {}
        for idx, row in cxp_df_normalized.iterrows():
            ref_number = clean_id(row.get('Ref #', ''))
            if ref_number:
                cxp_dict[ref_number] = row
        
        # Agregar columnas CXP
        cxp_columns = ['Date', 'Amt. Due']
        for col in cxp_columns:
            if col in cxp_df_normalized.columns:
                if col == 'Date':
                    consolidated_df['cxp_date'] = np.nan
                else:
                    consolidated_df[f'cxp_{col.lower().replace(" ", "_").replace(".", "")}'] = np.nan
        
        matched_cxp = 0
        if 'Asignacion' in consolidated_df.columns:
            for idx, row in consolidated_df.iterrows():
                asignacion = clean_id(row.get('Asignacion', ''))
                
                if asignacion and asignacion in cxp_dict:
                    cxp_row = cxp_dict[asignacion]
                    matched_cxp += 1
                    
                    for col in cxp_columns:
                        if col == 'Date':
                            date_value = cxp_row.get(col)
                            formatted_date = format_date_to_standard(date_value)
                            consolidated_df.loc[idx, 'cxp_date'] = formatted_date
                        else:
                            col_name = f'cxp_{col.lower().replace(" ", "_").replace(".", "")}'
                            consolidated_df.loc[idx, col_name] = cxp_row.get(col)
        
        st.success(f"✅ CXP procesado: {matched_cxp} matches por Asignacion")
    
    st.success(f"🎉 Consolidación completada: {len(consolidated_df)} registros finales")
    return consolidated_df

def insert_to_supabase(df):
    """Inserta los datos consolidados en Supabase"""
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
        status_text = st.empty()
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            try:
                result = supabase.table('orders').insert(batch).execute()
                total_inserted += len(batch)
                
                progress = min(1.0, (i + batch_size) / len(records))
                progress_bar.progress(progress)
                status_text.text(f"Insertando: {total_inserted}/{len(records)} registros")
                
            except Exception as batch_error:
                st.error(f"Error en lote {i//batch_size + 1}: {str(batch_error)}")
                continue
        
        progress_bar.progress(1.0)
        status_text.text(f"✅ Completado: {total_inserted} registros insertados")
        
        return total_inserted
        
    except Exception as e:
        st.error(f"Error general: {str(e)}")
        return 0

def verificar_conexion_supabase():
    """Verifica que la conexión a Supabase funcione correctamente"""
    if not supabase:
        return False, "No se pudo inicializar la conexión"
    
    try:
        result = supabase.table('orders').select('id').limit(1).execute()
        return True, "Conexión exitosa"
    except Exception as e:
        return False, str(e)

# ===============================================
# PÁGINAS DE LA APLICACIÓN
# ===============================================

def mostrar_consolidador(processing_mode):
    """Página del consolidador de archivos"""
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📁 Subir Archivos")
        
        drapify_file = st.file_uploader(
            "1. Archivo Drapify (OBLIGATORIO - Base de datos)",
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
        
        # Campo fecha manual para Logistics
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
    
    # Botón de procesamiento
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
                    logistics_matched = 0
                    if any(col.startswith('logistics_') for col in consolidated_df.columns):
                        logistics_cols = [col for col in consolidated_df.columns if col.startswith('logistics_')]
                        if logistics_cols:
                            logistics_matched = consolidated_df[logistics_cols[0]].notna().sum()
                    st.metric("Logistics Matched", logistics_matched)
                
                with col3:
                    aditionals_matched = 0
                    if any(col.startswith('aditionals_') for col in consolidated_df.columns):
                        aditionals_cols = [col for col in consolidated_df.columns if col.startswith('aditionals_')]
                        if aditionals_cols:
                            aditionals_matched = consolidated_df[aditionals_cols[0]].notna().sum()
                    st.metric("Aditionals Matched", aditionals_matched)
                
                with col4:
                    cxp_matched = 0
                    if any(col.startswith('cxp_') for col in consolidated_df.columns):
                        cxp_cols = [col for col in consolidated_df.columns if col.startswith('cxp_')]
                        if cxp_cols:
                            cxp_matched = consolidated_df[cxp_cols[0]].notna().sum()
                    st.metric("CXP Matched", cxp_matched)
                
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
                            else:
                                st.error("❌ Error insertando datos")
                
            except Exception as e:
                st.error(f"❌ Error procesando archivos: {str(e)}")
                st.exception(e)

def mostrar_calculo_utilidades():
    """Página de cálculo de utilidades"""
    st.title("💰 Cálculo de Utilidades")
    st.markdown("### Procesamiento automático según reglas de negocio")
    
    if not UTILIDADES_AVAILABLE:
        st.warning("⚠️ Módulo de utilidades no disponible")
        st.info("🚧 Esta funcionalidad estará disponible próximamente")
        return
    
    try:
        calculador = get_calculador_utilidades()
        st.success("✅ Módulo de utilidades cargado")
        
        # Mostrar TRM actual
        st.subheader("💱 TRM Actual en el Sistema")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("🇨🇴 Colombia", f"${calculador.trm_actual.get('colombia', 0):,.2f}")
        with col2:
            st.metric("🇵🇪 Perú", f"${calculador.trm_actual.get('peru', 0):,.2f}")
        with col3:
            st.metric("🇨🇱 Chile", f"${calculador.trm_actual.get('chile', 0):,.2f}")
        
        st.info("💡 Para cambiar TRM, ve a la página 'Gestión TRM'")
        
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")

def mostrar_gestion_trm():
    """Página de gestión de TRM - CONFIGURACIÓN MANUAL"""
    st.title("💱 Gestión de TRM")
    st.markdown("### Control Manual de Tasas Representativas del Mercado")
    
    if not UTILIDADES_AVAILABLE:
        st.warning("⚠️ Módulo de utilidades no disponible")
        st.info("🚧 Esta funcionalidad estará disponible próximamente")
        return
    
    try:
        calculador = get_calculador_utilidades()
        
        st.info("💡 **Importante:** Cambiar la TRM afecta TODOS los cálculos futuros de utilidades. Los cambios se aplican inmediatamente.")
        
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
                format="%.2f",
                key="trm_colombia"
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
                format="%.2f",
                key="trm_peru"
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
                format="%.2f",
                key="trm_chile"
            )
            st.caption(f"Actual: ${calculador.trm_actual.get('chile', 0):,.2f}")
        
        # BOTÓN PARA ACTUALIZAR
        st.markdown("---")
        col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
        
        with col_btn2:
            if st.button("💾 ACTUALIZAR TRM", type="primary", use_container_width=True):
                # Preparar nuevas TRM
                nuevas_trm = {
                    'colombia': nueva_trm_colombia,
                    'peru': nueva_trm_peru,
                    'chile': nueva_trm_chile
                }
                
                with st.spinner("Actualizando TRM en la base de datos..."):
                    if calculador.actualizar_trm(nuevas_trm, "usuario_manual"):
                        st.success("✅ ¡TRM actualizada exitosamente!")
                        st.balloons()
                        time.sleep(2)
                        st.rerun()
                    else:
                        st.error("❌ Error al actualizar TRM")
        
        # MOSTRAR CAMBIOS
        st.subheader("📊 Cambios Propuestos")
        cambios_df = pd.DataFrame({
            'País': ['🇨🇴 Colombia', '🇵🇪 Perú', '🇨🇱 Chile'],
            'TRM Actual': [
                f"${calculador.trm_actual.get('colombia', 0):,.2f}",
                f"${calculador.trm_actual.get('peru', 0):,.2f}",
                f"${calculador.trm_actual.get('chile', 0):,.2f}"
            ],
            'TRM Nueva': [
                f"${nueva_trm_colombia:,.2f}",
                f"${nueva_trm_peru:,.2f}",
                f"${nueva_trm_chile:,.2f}"
            ],
            'Cambio': [
                f"{((nueva_trm_colombia - calculador.trm_actual.get('colombia', 0)) / calculador.trm_actual.get('colombia', 1)) * 100:+.2f}%",
                f"{((nueva_trm_peru - calculador.trm_actual.get('peru', 0)) / calculador.trm_actual.get('peru', 1)) * 100:+.2f}%",
                f"{((nueva_trm_chile - calculador.trm_actual.get('chile', 0)) / calculador.trm_actual.get('chile', 1)) * 100:+.2f}%"
            ]
        })
        
        st.dataframe(cambios_df, use_container_width=True, hide_index=True)
        
        # HISTORIAL
        st.subheader("📋 Últimos Cambios")
        try:
            result = supabase.table('trm_history').select('*').order('fecha_cambio', desc=True).limit(5).execute()
            
            if result.data:
                historial_df = pd.DataFrame(result.data)
                historial_df['fecha_cambio'] = pd.to_datetime(historial_df['fecha_cambio']).dt.strftime('%Y-%m-%d %H:%M')
                st.dataframe(historial_df[['fecha_cambio', 'pais', 'valor_anterior', 'valor_nuevo', 'cambio_porcentual', 'usuario']], use_container_width=True, hide_index=True)
            else:
                st.info("No hay cambios registrados")
        except Exception as e:
            st.error(f"Error cargando historial: {str(e)}")
            
    except Exception as e:
        st.error(f"❌ Error inicializando gestión TRM: {str(e)}")

def mostrar_dashboard_utilidades():
    """Dashboard de utilidades"""
    st.title("📊 Dashboard de Utilidades")
    st.markdown("### Panel de control y métricas")
    
    st.info("📝 No hay datos de utilidades para mostrar")
    st.markdown("💡 Primero calcula utilidades en la página 'Cálculo de Utilidades'")

def mostrar_reportes():
    """Página de reportes"""
    st.title("📋 Reportes")
    st.markdown("### Generación de reportes automáticos")
    
    st.info("🚧 Funcionalidad en desarrollo")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Tipos de Reportes")
        st.write("• Reporte Mensual Ejecutivo")
        st.write("• Análisis Comparativo") 
        st.write("• Tendencias por Cuenta")
        st.write("• Performance Detallado")
    
    with col2:
        st.subheader("📁 Formatos Disponibles")
        st.write("• Excel (.xlsx)")
        st.write("• PDF Ejecutivo")
        st.write("• CSV Detallado")
        st.write("• Dashboard Interactivo")

# ===============================================
# FUNCIÓN PRINCIPAL
# ===============================================

def main():
    st.title("💰 Sistema de Gestión Integral")
    st.markdown("### Consolidación de archivos y cálculo de utilidades")
    
    # Verificar conexión Supabase
    conexion_ok, mensaje_conexion = verificar_conexion_supabase()
    
    # Sidebar
    with st.sidebar:
        st.image("https://via.placeholder.com/150x50/4F46E5/white?text=LOGO", width=150)
        st.markdown("---")
        
        # Estado de conexión
        if conexion_ok:
            st.success("✅ Supabase conectado")
        else:
            st.error("❌ Sin conexión BD")
        
        st.markdown("---")
        
        # NAVEGACIÓN - SIEMPRE MOSTRAR TODAS LAS OPCIONES
        opciones_menu = [
            "🏠 Consolidador de Archivos",
            "💰 Cálculo de Utilidades",
            "💱 Gestión TRM", 
            "📊 Dashboard Utilidades",
            "📋 Reportes"
        ]
        
        pagina = st.selectbox("📋 Navegación", opciones_menu)
        
        st.markdown("---")
        
        # Configuración
        processing_mode = st.radio(
            "Modo de procesamiento:",
            ["Solo consolidar", "Consolidar e insertar en DB"]
        )
        
        st.markdown("---")
        st.markdown("**📋 Orden de procesamiento:**")
        st.markdown("1. 📋 **Drapify** (base - obligatorio)")
        st.markdown("2. 🚚 **Logistics** (opcional)")
        st.markdown("3. ➕ **Aditionals** (opcional)")
        st.markdown("4. 🏷️ **Calcular Asignacion**")
        st.markdown("5. 💰 **CXP** (opcional)")
    
    # Routing de páginas
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

# ===============================================
# EJECUTAR LA APLICACIÓN
# ===============================================

if __name__ == "__main__":
    main()
