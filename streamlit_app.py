import streamlit as st
import pandas as pd
import numpy as np
from supabase import create_client, Client
import os
from datetime import datetime, timedelta
import io
import time
import re
import hashlib

# Configuración de la página
st.set_page_config(
    page_title="Consolidador de Órdenes",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuración de Supabase con credenciales integradas
@st.cache_resource
def init_supabase():
    url = "https://pvbzzpeyhhxexyabizbv.supabase.co"
    key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB2Ynp6cGV5aGh4ZXh5YWJpemJ2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM5OTc5ODcsImV4cCI6MjA2OTU3Mzk4N30.06S8jDjNReAd6Oj8AZvOS2PUcO2ASJHVA3VUNYVeAR4"
    return create_client(url, key)

supabase = init_supabase()

# Test de conexión al inicio
try:
    test_result = supabase.table('consolidated_orders').select('id').limit(1).execute()
    st.sidebar.success("✅ Conectado a Supabase")
except Exception as e:
    st.sidebar.error(f"❌ Error de conexión: {str(e)}")

# FUNCIONES DE UTILIDAD

def calculate_file_hash(file):
    """Calcula el hash MD5 de un archivo para detectar duplicados"""
    file.seek(0)
    file_hash = hashlib.md5(file.read()).hexdigest()
    file.seek(0)  # Reset file position
    return file_hash

def check_file_already_processed(file_hash):
    """Verifica si un archivo ya fue procesado anteriormente"""
    try:
        result = supabase.table('processing_logs').select('*').eq('file_hash', file_hash).execute()
        return len(result.data) > 0, result.data[0] if result.data else None
    except:
        return False, None

def get_recent_uploads(limit=10):
    """Obtiene los últimos archivos subidos"""
    try:
        result = supabase.table('processing_logs').select('*').order('processing_date', desc=True).limit(limit).execute()
        return result.data
    except:
        return []

def fix_encoding(text):
    """Corrige caracteres mal codificados automáticamente"""
    if pd.isna(text) or not isinstance(text, str):
        return text
    
    try:
        if 'Ã' in text:
            fixed = text.encode('latin-1').decode('utf-8')
            return fixed
    except:
        pass
    
    return text

def format_currency_no_decimals(value):
    """Formato currency sin decimales: $#,##0"""
    if pd.isna(value):
        return None
    try:
        num_value = float(value)
        rounded_value = round(num_value)
        return f"${rounded_value:,}"
    except:
        return value

def format_currency_with_decimals(value):
    """Formato currency con decimales: $#,##0.00"""
    if pd.isna(value):
        return None
    try:
        num_value = float(value)
        return f"${num_value:,.2f}"
    except:
        return value

def format_date_standard(date_value, input_format="auto"):
    """Convierte fechas a formato YYYY-MM-DD"""
    if pd.isna(date_value) or date_value == "":
        return None
    
    date_str = str(date_value).strip()
    
    try:
        if re.match(r'\d{4}-\d{2}-\d{2}\s', date_str):
            return date_str.split(' ')[0]
        
        if re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_str):
            parts = date_str.split('/')
            if len(parts) == 3:
                month = parts[0].zfill(2)
                day = parts[1].zfill(2)
                year = parts[2]
                return f"{year}-{month}-{day}"
        
        if re.match(r'\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
            
    except:
        pass
    
    return date_str

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

def map_column_names(df):
    """Mapea nombres de columnas del CSV a los nombres de la base de datos"""
    column_mapping = {
        'System#': 'system_number',
        'Serial#': 'serial_number',
        'order_id': 'order_id',
        'pack_id': 'pack_id',
        'ASIN': 'asin',
        'client_first_name': 'client_first_name',
        'client_last_name': 'client_last_name',
        'client_doc_id': 'client_doc_id',
        'account_name': 'account_name',
        'date_created': 'date_created',
        'quantity': 'quantity',
        'title': 'title',
        'unit_price': 'unit_price',
        'logistic_type': 'logistic_type',
        'address_line': 'address_line',
        'street_name': 'street_name',
        'street_number': 'street_number',
        'city': 'city',
        'state': 'state',
        'country': 'country',
        'receiver_phone': 'receiver_phone',
        'amz_order_id': 'amz_order_id',
        'prealert_id': 'prealert_id',
        'ETIQUETA_ENVIO': 'etiqueta_envio',
        'order_status_meli': 'order_status_meli',
        'Declare Value': 'declare_value',
        'Meli Fee': 'meli_fee',
        'IVA': 'iva',
        'ICA': 'ica',
        'FUENTE': 'fuente',
        'senders_cost': 'senders_cost',
        'gross_amount': 'gross_amount',
        'net_received_amount': 'net_received_amount',
        'nombre_del_tercero': 'nombre_del_tercero',
        'direccion': 'direccion',
        'apelido_del_tercero': 'apelido_del_tercero',
        'Estado': 'estado',
        'razon_social': 'razon_social',
        'Ciudad': 'ciudad',
        'Numero de documento': 'numero_de_documento',
        'digital_verification': 'digital_verification',
        'tipo': 'tipo',
        'telefono': 'telefono',
        'giro': 'giro',
        'correo': 'correo',
        'net_real_amount': 'net_real_amount',
        'logistic_weight_lbs': 'logistic_weight_lbs',
        'refunded_date': 'refunded_date',
        'Asignacion': 'asignacion',
    }
    
    renamed_df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    return renamed_df

def apply_basic_formatting(df):
    """Aplica formatos básicos sin afectar campos numéricos para BD"""
    text_columns = [
        'client_first_name', 'client_last_name', 'title', 'address_line', 
        'street_name', 'city', 'state', 'country', 'nombre_del_tercero',
        'direccion', 'apelido_del_tercero', 'estado', 'razon_social', 'ciudad',
        'logistics_description', 'logistics_shipper', 'logistics_consignee',
        'logistics_country', 'logistics_state', 'logistics_city', 'logistics_address'
    ]
    
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].apply(fix_encoding)
    
    date_columns = {
        'date_created': 'datetime',
        'cxp_date': 'cxp_format'
    }
    
    for col, format_type in date_columns.items():
        if col in df.columns:
            df[col] = df[col].apply(format_date_standard)
    
    return df

def apply_display_formatting(df):
    """Aplica formatos de visualización (currency) solo para descarga CSV"""
    display_df = df.copy()
    
    currency_no_decimals_columns = [
        'unit_price', 'meli_fee', 'iva', 'ica', 'fuente', 
        'senders_cost', 'gross_amount', 'net_received_amount', 'net_real_amount',
        'order_cost', 'Meli Fee', 'IVA', 'ICA', 'FUENTE'
    ]
    
    for col in currency_no_decimals_columns:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(format_currency_no_decimals)
    
    currency_with_decimals_columns = [
        'profit_price', 'declare_value', 'data_base_price',
        'logistics_fob', 'logistics_weight', 'logistics_length', 'logistics_width', 
        'logistics_height', 'logistics_insurance', 'logistics_logistics',
        'logistics_duties_prealert', 'logistics_duties_pay', 'logistics_duty_fee',
        'logistics_saving', 'logistics_total',
        'cxp_co_aereo', 'cxp_arancel', 'cxp_iva', 'cxp_handling', 
        'cxp_dest_delivery', 'cxp_amt_due', 'cxp_goods_value'
    ]
    
    for col in currency_with_decimals_columns:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(format_currency_with_decimals)
    
    return display_df

# NUEVAS FUNCIONES PARA PROCESAMIENTO INCREMENTAL

def process_drapify_file(drapify_df):
    """Procesa archivo Drapify y actualiza/inserta registros"""
    st.info("📋 Procesando archivo Drapify...")
    
    # Aplicar formatos básicos
    drapify_df = apply_basic_formatting(drapify_df)
    
    # Calcular Asignacion si es posible
    if 'account_name' in drapify_df.columns and 'Serial#' in drapify_df.columns:
        drapify_df['Asignacion'] = drapify_df.apply(
            lambda row: calculate_asignacion(row['account_name'], row['Serial#']), 
            axis=1
        )
    
    # Mapear columnas
    df_mapped = map_column_names(drapify_df)
    
    # Filtrar columnas base de Drapify
    base_columns = [
        'system_number', 'serial_number', 'order_id', 'pack_id', 'asin',
        'client_first_name', 'client_last_name', 'client_doc_id', 'account_name',
        'date_created', 'quantity', 'title', 'unit_price', 'logistic_type',
        'address_line', 'street_name', 'street_number', 'city', 'state', 'country',
        'receiver_phone', 'amz_order_id', 'prealert_id', 'etiqueta_envio',
        'order_status_meli', 'declare_value', 'meli_fee', 'iva', 'ica', 'fuente',
        'senders_cost', 'gross_amount', 'net_received_amount', 'nombre_del_tercero',
        'direccion', 'apelido_del_tercero', 'estado', 'razon_social', 'ciudad',
        'numero_de_documento', 'digital_verification', 'tipo', 'telefono', 'giro',
        'correo', 'net_real_amount', 'logistic_weight_lbs', 'refunded_date',
        'asignacion'
    ]
    
    df_filtered = df_mapped[[col for col in base_columns if col in df_mapped.columns]]
    
    return df_filtered

def process_logistics_file(logistics_df, logistics_date):
    """Procesa archivo Logistics con fecha asignada"""
    st.info(f"🚚 Procesando archivo Logistics con fecha: {logistics_date}")
    
    # Agregar la columna de fecha
    logistics_df['logistics_date'] = logistics_date
    
    # Preparar diccionarios para matching
    logistics_dict_by_reference = {}
    logistics_dict_by_order_number = {}
    
    for idx, row in logistics_df.iterrows():
        reference = clean_id(row.get('Reference', ''))
        order_number = clean_id(row.get('Order number', ''))
        
        row_data = row.to_dict()
        row_data['logistics_date'] = logistics_date
        
        if reference:
            logistics_dict_by_reference[reference] = row_data
        if order_number:
            logistics_dict_by_order_number[order_number] = row_data
    
    return logistics_dict_by_reference, logistics_dict_by_order_number

def process_aditionals_file(aditionals_df):
    """Procesa archivo Aditionals"""
    st.info("➕ Procesando archivo Aditionals...")
    
    aditionals_dict = {}
    for idx, row in aditionals_df.iterrows():
        order_id = clean_id(row.get('Order Id', ''))
        if order_id:
            aditionals_dict[order_id] = row.to_dict()
    
    return aditionals_dict

def process_cxp_file(cxp_df):
    """Procesa archivo CXP"""
    st.info("💰 Procesando archivo CXP...")
    
    # Normalizar nombres de columnas
    column_mapping = {
        'OT Number': 'OT Number',
        'Date': 'Date', 
        'Ref #': 'Ref #',
        'Consignee': 'Consignee',
        'CO Aereo': 'CO Aereo',
        'Arancel': 'Arancel',
        'IVA': 'IVA',
        'Handling': 'Handling',
        'Dest. Delivery': 'Dest. Delivery',
        'Amt. Due': 'Amt. Due',
        'Goods Value': 'Goods Value',
    }
    
    cxp_df_normalized = cxp_df.rename(columns=column_mapping)
    
    cxp_dict = {}
    for idx, row in cxp_df_normalized.iterrows():
        ref_number = clean_id(row.get('Ref #', ''))
        if ref_number:
            cxp_dict[ref_number] = row.to_dict()
    
    return cxp_dict

def upsert_to_database(data, file_type, file_name, file_hash):
    """Inserta o actualiza registros en la base de datos"""
    try:
        if file_type == 'drapify':
            # Para Drapify, hacer upsert por order_id
            records = data.to_dict('records')
            
            # Convertir tipos de datos
            integer_columns = ['system_number', 'quantity', 'iva', 'ica']
            for record in records:
                for key, value in record.items():
                    if pd.isna(value) or value is None:
                        record[key] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        record[key] = value.strftime('%Y-%m-%d') if hasattr(value, 'strftime') else str(value)
                    elif key in integer_columns:
                        try:
                            if str(value) == "0.0" or str(value) == "":
                                record[key] = 0
                            else:
                                record[key] = int(float(value))
                        except:
                            record[key] = 0
                    else:
                        record[key] = str(value)
            
            # Hacer upsert en lotes
            batch_size = 50
            total_processed = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                # Usar upsert para actualizar si existe o insertar si no existe
                result = supabase.table('consolidated_orders').upsert(
                    batch, 
                    on_conflict='order_id'
                ).execute()
                
                total_processed += len(batch)
            
            # Log del procesamiento
            log_data = {
                'file_type': file_type,
                'file_name': file_name,
                'file_hash': file_hash,
                'records_processed': len(records),
                'records_matched': total_processed,
                'status': 'success'
            }
            supabase.table('processing_logs').insert(log_data).execute()
            
            return total_processed
            
        else:
            # Para otros tipos de archivo, actualizar registros existentes
            if file_type == 'logistics':
                return update_logistics_data(data, file_name, file_hash)
            elif file_type == 'aditionals':
                return update_aditionals_data(data, file_name, file_hash)
            elif file_type == 'cxp':
                return update_cxp_data(data, file_name, file_hash)
                
    except Exception as e:
        st.error(f"Error en base de datos: {str(e)}")
        return 0

def update_logistics_data(logistics_dicts, file_name, file_hash):
    """Actualiza datos de logistics en registros existentes"""
    logistics_dict_by_reference, logistics_dict_by_order_number = logistics_dicts
    
    # Obtener todos los order_ids y prealert_ids existentes
    existing_records = supabase.table('consolidated_orders').select('id,order_id,prealert_id').execute()
    
    updates_count = 0
    batch_updates = []
    
    for record in existing_records.data:
        order_id = clean_id(record.get('order_id', ''))
        prealert_id = clean_id(record.get('prealert_id', ''))
        record_id = record.get('id')
        
        logistics_data = None
        
        # Buscar match
        if order_id and order_id in logistics_dict_by_reference:
            logistics_data = logistics_dict_by_reference[order_id]
        elif prealert_id and prealert_id in logistics_dict_by_order_number:
            logistics_data = logistics_dict_by_order_number[prealert_id]
        
        if logistics_data:
            # Preparar datos para actualizar
            update_data = {'id': record_id}
            
            # Mapear columnas de logistics
            logistics_mapping = {
                'Guide Number': 'logistics_guide_number',
                'Order number': 'logistics_order_number',
                'Reference': 'logistics_reference',
                'logistics_date': 'logistics_date',
                # ... agregar resto del mapeo
            }
            
            for orig_col, db_col in logistics_mapping.items():
                if orig_col in logistics_data:
                    update_data[db_col] = logistics_data[orig_col]
            
            batch_updates.append(update_data)
            updates_count += 1
            
            # Actualizar en lotes
            if len(batch_updates) >= 50:
                supabase.table('consolidated_orders').upsert(batch_updates).execute()
                batch_updates = []
    
    # Actualizar últimos registros
    if batch_updates:
        supabase.table('consolidated_orders').upsert(batch_updates).execute()
    
    # Log
    log_data = {
        'file_type': 'logistics',
        'file_name': file_name,
        'file_hash': file_hash,
        'records_processed': len(logistics_dict_by_reference) + len(logistics_dict_by_order_number),
        'records_matched': updates_count,
        'status': 'success'
    }
    supabase.table('processing_logs').insert(log_data).execute()
    
    return updates_count

def update_aditionals_data(aditionals_dict, file_name, file_hash):
    """Actualiza datos de aditionals en registros existentes"""
    # Similar a logistics pero con la lógica de aditionals
    # Por brevedad, estructura similar
    pass

def update_cxp_data(cxp_dict, file_name, file_hash):
    """Actualiza datos de CXP en registros existentes"""
    # Similar a logistics pero con la lógica de CXP
    # Por brevedad, estructura similar
    pass

# INTERFAZ PRINCIPAL
def main():
    st.title("📦 Consolidador de Órdenes - Sistema Incremental")
    st.markdown("### Carga archivos de forma independiente y flexible")
    
    # Sidebar
    with st.sidebar:
        st.header("📊 Panel de Control")
        
        # Mostrar últimos archivos subidos
        st.subheader("📋 Últimos archivos procesados")
        recent_uploads = get_recent_uploads()
        
        if recent_uploads:
            for upload in recent_uploads:
                st.write(f"**{upload.get('file_type', 'N/A')}**: {upload.get('file_name', 'N/A')}")
                st.caption(f"📅 {upload.get('processing_date', 'N/A')}")
                st.caption(f"📊 {upload.get('records_matched', 0)} registros")
                st.divider()
        else:
            st.info("No hay archivos procesados aún")
        
        st.markdown("---")
        
        # Estadísticas generales
        try:
            total_records = supabase.table('consolidated_orders').select('count', count='exact').execute()
            st.metric("Total Registros en BD", total_records.count)
        except:
            st.metric("Total Registros en BD", "Error")
    
    # Tabs para diferentes tipos de archivo
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📋 Drapify", "🚚 Logistics", "➕ Aditionals", "💰 CXP", "📊 Consultas"])
    
    # TAB DRAPIFY
    with tab1:
        st.header("📋 Cargar archivo Drapify")
        st.info("Archivo base con información de órdenes. Se actualizarán registros existentes.")
        
        drapify_file = st.file_uploader(
            "Selecciona archivo Drapify",
            type=['xlsx', 'xls', 'csv'],
            key="drapify"
        )
        
        if drapify_file:
            # Verificar si ya fue procesado
            file_hash = calculate_file_hash(drapify_file)
            already_processed, prev_log = check_file_already_processed(file_hash)
            
            if already_processed:
                st.warning(f"⚠️ Este archivo ya fue procesado el {prev_log.get('processing_date')}")
                if not st.checkbox("Procesar de todos modos", key="force_drapify"):
                    st.stop()
            
            if st.button("🚀 Procesar Drapify", type="primary", key="process_drapify"):
                with st.spinner("Procesando archivo Drapify..."):
                    try:
                        # Leer archivo
                        if drapify_file.name.endswith('.csv'):
                            df = pd.read_csv(drapify_file)
                        else:
                            df = pd.read_excel(drapify_file)
                        
                        st.success(f"✅ Archivo cargado: {len(df)} registros")
                        
                        # Procesar
                        processed_df = process_drapify_file(df)
                        
                        # Preview
                        with st.expander("👀 Vista previa de datos"):
                            st.dataframe(processed_df.head(10))
                        
                        # Guardar en BD
                        inserted = upsert_to_database(
                            processed_df, 
                            'drapify', 
                            drapify_file.name,
                            file_hash
                        )
                        
                        st.success(f"✅ {inserted} registros procesados exitosamente")
                        st.balloons()
                        
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
    
    # TAB LOGISTICS
    with tab2:
        st.header("🚚 Cargar archivo Logistics")
        st.info("Costos de Anicam para envíos internacionales")
        
        logistics_file = st.file_uploader(
            "Selecciona archivo Logistics",
            type=['xlsx', 'xls', 'csv'],
            key="logistics"
        )
        
        if logistics_file:
            # Selector de fecha
            st.subheader("📅 Selecciona fecha para este archivo")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📅 Hoy", key="today_logistics"):
                    st.session_state.logistics_date = datetime.now().date()
            
            with col2:
                if st.button("📅 Ayer", key="yesterday_logistics"):
                    st.session_state.logistics_date = (datetime.now() - timedelta(days=1)).date()
            
            with col3:
                custom_date = st.date_input(
                    "Fecha personalizada",
                    value=st.session_state.get('logistics_date', datetime.now().date()),
                    key="custom_date_logistics"
                )
                st.session_state.logistics_date = custom_date
            
            st.info(f"📅 Fecha seleccionada: **{st.session_state.logistics_date}**")
            
            # Verificar duplicado
            file_hash = calculate_file_hash(logistics_file)
            already_processed, prev_log = check_file_already_processed(file_hash)
            
            if already_processed:
                st.warning(f"⚠️ Este archivo ya fue procesado el {prev_log.get('processing_date')}")
                if not st.checkbox("Procesar de todos modos", key="force_logistics"):
                    st.stop()
            
            if st.button("🚀 Procesar Logistics", type="primary", key="process_logistics"):
                with st.spinner("Procesando archivo Logistics..."):
                    try:
                        # Leer archivo
                        if logistics_file.name.endswith('.csv'):
                            df = pd.read_csv(logistics_file)
                        else:
                            df = pd.read_excel(logistics_file)
                        
                        st.success(f"✅ Archivo cargado: {len(df)} registros")
                        
                        # Procesar con fecha
                        logistics_dicts = process_logistics_file(df, st.session_state.logistics_date)
                        
                        # Actualizar en BD
                        updated = update_logistics_data(
                            logistics_dicts,
                            logistics_file.name,
                            file_hash
                        )
                        
                        st.success(f"✅ {updated} registros actualizados con datos de Logistics")
                        st.balloons()
                        
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
    
    # TAB ADITIONALS
    with tab3:
        st.header("➕ Cargar archivo Aditionals")
        st.info("Costos adicionales de Anicam")
        
        aditionals_file = st.file_uploader(
            "Selecciona archivo Aditionals",
            type=['xlsx', 'xls', 'csv'],
            key="aditionals"
        )
        
        if aditionals_file:
            # Similar a logistics pero sin selector de fecha
            st.info("Procesamiento similar a Logistics...")
    
    # TAB CXP
    with tab4:
        st.header("💰 Cargar archivo CXP")
        st.info("Costos de Chilexpress")
        
        cxp_file = st.file_uploader(
            "Selecciona archivo CXP",
            type=['xlsx', 'xls', 'csv'],
            key="cxp"
        )
        
        if cxp_file:
            # Similar a logistics pero sin selector de fecha
            st.info("Procesamiento similar a Logistics...")
    
    # TAB CONSULTAS
    with tab5:
        st.header("🔍 Consultar Datos")
        
        # Búsqueda
        col1, col2 = st.columns(2)
        
        with col1:
            search_order_id = st.text_input("Buscar por Order ID")
        
        with col2:
            search_prealert_id = st.text_input("Buscar por Prealert ID")
        
        if st.button("🔍 Buscar"):
            try:
                query = supabase.table('consolidated_orders').select('*')
                
                if search_order_id:
                    query = query.eq('order_id', search_order_id)
                
                if search_prealert_id:
                    query = query.eq('prealert_id', search_prealert_id)
                
                result = query.execute()
                
                if result.data:
                    df = pd.DataFrame(result.data)
                    st.success(f"✅ Encontrados {len(df)} registros")
                    
                    # Aplicar formatos para visualización
                    display_df = apply_display_formatting(df)
                    st.dataframe(display_df, use_container_width=True)
                    
                    # Descargar
                    csv = display_df.to_csv(index=False)
                    st.download_button(
                        "📥 Descargar CSV",
                        csv,
                        f"busqueda_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        "text/csv"
                    )
                else:
                    st.warning("No se encontraron registros")
                    
            except Exception as e:
                st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
