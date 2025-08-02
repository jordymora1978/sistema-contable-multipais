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

# FUNCIONES DE FORMATO Y LIMPIEZA

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

def check_existing_data():
    """Verifica si hay datos existentes en la tabla"""
    try:
        result = supabase.table('consolidated_orders').select('id').limit(1).execute()
        return len(result.data) > 0
    except:
        return False

def clear_existing_data():
    """Elimina todos los registros existentes de las tablas"""
    try:
        supabase.table('consolidated_orders').delete().neq('id', 0).execute()
        supabase.table('processing_logs').delete().neq('id', 0).execute()
        return True
    except Exception as e:
        st.error(f"Error limpiando datos: {str(e)}")
        return False

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

def clean_id_aggressive(value):
    """Limpieza más agresiva para IDs corruptos"""
    if pd.isna(value):
        return None
    
    str_value = str(value).strip()
    str_value = str_value.replace("'", "")
    str_value = str_value.replace('"', "")
    str_value = str_value.replace(" ", "")
    str_value = str_value.replace("\t", "")
    str_value = str_value.replace("\n", "")
    str_value = str_value.replace(".", "")
    
    if str_value.endswith('0') and len(str_value) > 1:
        original = str(value)
        if '.0' in original:
            str_value = str_value[:-1]
    
    return str_value if str_value and str_value != 'nan' else None

def clean_numeric_value(value):
    """Limpia valores numéricos, eliminando basura como 'XXXXXXXXXX'"""
    if pd.isna(value) or value is None:
        return None
    
    str_value = str(value).strip()
    
    garbage_values = [
        'XXXXXXXXXX', 'XXXXXXX', 'XXXXX', 'XXX',
        'N/A', 'n/a', 'NA', 'na',
        '-', '--', '---',
        '#N/A', '#VALUE!', '#REF!',
        'null', 'NULL', 'Null',
        '', ' '
    ]
    
    if str_value in garbage_values:
        return None
    
    try:
        clean_value = str_value.replace('$', '').replace(',', '').replace(' ', '')
        return float(clean_value)
    except:
        return None

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
    
    st.info("🔧 Aplicando formatos básicos para base de datos...")
    
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
    
    st.success("✅ Formatos básicos aplicados")
    return df

def apply_display_formatting(df):
    """Aplica formatos de visualización (currency) solo para descarga CSV"""
    
    st.info("🎨 Aplicando formatos de visualización para descarga...")
    
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
    
    st.success("✅ Formatos de visualización aplicados")
    return display_df

def process_files_according_to_rules(drapify_df, logistics_df=None, aditionals_df=None, cxp_df=None, logistics_date=None):
    """
    Procesa y consolida todos los archivos según las reglas especificadas
    """
    
    st.info("🔄 Iniciando consolidación según reglas especificadas...")
    
    # PASO 1: Usar Drapify como base
    consolidated_df = drapify_df.copy()
    st.success(f"✅ Archivo base Drapify procesado: {len(consolidated_df)} registros")
    
    # PASO 2: Procesar archivo Logistics si está disponible
    if logistics_df is not None and not logistics_df.empty:
        st.info("🚚 Procesando archivo Logistics...")
        
        # Agregar columna de fecha si se especificó
        if logistics_date:
            logistics_df['logistics_date'] = logistics_date
        
        # Crear diccionario para mapeo rápido con limpieza agresiva
        logistics_dict_by_reference = {}
        logistics_dict_by_order_number = {}
        
        for idx, row in logistics_df.iterrows():
            reference = clean_id_aggressive(row.get('Reference', ''))
            order_number = clean_id_aggressive(row.get('Order number', ''))
            
            if reference:
                logistics_dict_by_reference[reference] = row
            if order_number:
                logistics_dict_by_order_number[order_number] = row
        
        st.info(f"📋 Logistics indexado: {len(logistics_dict_by_reference)} por Reference, {len(logistics_dict_by_order_number)} por Order number")
        
        # Agregar columnas de Logistics
        logistics_columns = [
            'Guide Number', 'Order number', 'Reference', 'SAP Code', 'Invoice', 
            'Status', 'FOB', 'Unit', 'Weight', 'Length', 'Width', 'Height',
            'Insurance', 'Logistics', 'Duties Prealert', 'Duties Pay', 
            'Duty Fee', 'Saving', 'Total', 'Description', 'Shipper', 'Phone',
            'Consignee', 'Identification', 'Country', 'State', 'City', 
            'Address', 'Master Guide', 'Tariff Position', 'External Id', 'Invoice'
        ]
        
        # Agregar columna de fecha si existe
        if logistics_date:
            consolidated_df['logistics_date'] = np.nan
        
        for col in logistics_columns:
            if col in logistics_df.columns:
                consolidated_df[f'logistics_{col.lower().replace(" ", "_")}'] = np.nan
        
        matched_by_order_id = 0
        matched_by_prealert_id = 0
        no_match_count = 0
        
        # Hacer el matching con la lógica mejorada
        for idx, row in consolidated_df.iterrows():
            order_id = clean_id_aggressive(row.get('order_id', ''))
            prealert_id = clean_id_aggressive(row.get('prealert_id', ''))
            
            logistics_row = None
            match_type = None
            
            # Primero intenta con order_id
            if order_id and order_id in logistics_dict_by_reference:
                logistics_row = logistics_dict_by_reference[order_id]
                matched_by_order_id += 1
                match_type = "order_id->Reference"
            
            # Si no encontró, intenta con prealert_id
            elif prealert_id and prealert_id in logistics_dict_by_order_number:
                logistics_row = logistics_dict_by_order_number[prealert_id]
                matched_by_prealert_id += 1
                match_type = "prealert_id->Order number"
            
            # Si encontró match, copiar los datos
            if logistics_row is not None:
                for col in logistics_columns:
                    if col in logistics_df.columns:
                        # Convertir Series a dict si es necesario
                        if isinstance(logistics_row, pd.Series):
                            value = logistics_row.get(col)
                        else:
                            value = logistics_row.get(col)
                        consolidated_df.loc[idx, f'logistics_{col.lower().replace(" ", "_")}'] = value
                
                # Agregar fecha si existe
                if logistics_date:
                    consolidated_df.loc[idx, 'logistics_date'] = logistics_date
                
                if (matched_by_order_id + matched_by_prealert_id) <= 5:
                    st.write(f"✅ Match {matched_by_order_id + matched_by_prealert_id}: {match_type} - order_id: {order_id}, prealert_id: {prealert_id}")
            else:
                no_match_count += 1
        
        st.success(f"✅ Logistics procesado: {matched_by_order_id} por order_id, {matched_by_prealert_id} por prealert_id, {no_match_count} sin match")
    
    # PASO 3: Procesar archivo Aditionals si está disponible
    if aditionals_df is not None and not aditionals_df.empty:
        st.info("➕ Procesando archivo Aditionals...")
        
        aditionals_dict = {}
        for idx, row in aditionals_df.iterrows():
            order_id = clean_id_aggressive(row.get('Order Id', ''))
            if order_id:
                aditionals_dict[order_id] = row
        
        st.info(f"📋 Aditionals indexado: {len(aditionals_dict)} registros")
        
        aditionals_columns = ['Order Id', 'Item', 'Reference', 'Description', 'Quantity', 'UnitPrice', 'Total']
        
        for col in aditionals_columns:
            if col in aditionals_df.columns:
                consolidated_df[f'aditionals_{col.lower().replace(" ", "_")}'] = np.nan
        
        matched_aditionals = 0
        
        for idx, row in consolidated_df.iterrows():
            prealert_id = clean_id_aggressive(row.get('prealert_id', ''))
            
            if prealert_id and prealert_id in aditionals_dict:
                aditionals_row = aditionals_dict[prealert_id]
                matched_aditionals += 1
                
                for col in aditionals_columns:
                    if col in aditionals_df.columns:
                        consolidated_df.loc[idx, f'aditionals_{col.lower().replace(" ", "_")}'] = aditionals_row.get(col)
                
                if matched_aditionals <= 5:
                    st.write(f"✅ Aditional Match {matched_aditionals}: prealert_id {prealert_id} encontrado")
        
        st.success(f"✅ Aditionals procesado: {matched_aditionals} matches por prealert_id")
    
    # PASO 4: Calcular columna Asignacion
    st.info("🏷️ Calculando columna Asignacion...")
    
    if 'account_name' in consolidated_df.columns and 'Serial#' in consolidated_df.columns:
        consolidated_df['Asignacion'] = consolidated_df.apply(
            lambda row: calculate_asignacion(row['account_name'], row['Serial#']), 
            axis=1
        )
        asignaciones_calculadas = consolidated_df['Asignacion'].notna().sum()
        st.success(f"✅ Asignaciones calculadas: {asignaciones_calculadas}")
    else:
        st.warning("⚠️ No se pudo calcular Asignacion: faltan columnas account_name o Serial#")
    
    # PASO 5: Procesar archivo CXP si está disponible
    if cxp_df is not None and not cxp_df.empty:
        st.info("💰 Procesando archivo CXP...")
        
        st.write(f"🔍 Columnas encontradas en CXP: {list(cxp_df.columns)}")
        
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
            ref_number = clean_id_aggressive(row.get('Ref #', ''))
            if ref_number:
                cxp_dict[ref_number] = row
        
        st.info(f"📋 CXP indexado: {len(cxp_dict)} registros")
        
        cxp_refs = list(cxp_dict.keys())[:5]
        st.write(f"🔍 Ejemplos de Ref # en CXP: {cxp_refs}")
        
        available_cxp_columns = []
        standard_cxp_columns = ['OT Number', 'Date', 'Ref #', 'Consignee', 'CO Aereo', 
                               'Arancel', 'IVA', 'Handling', 'Dest. Delivery', 'Amt. Due', 'Goods Value']
        
        for col in standard_cxp_columns:
            if col in cxp_df_normalized.columns:
                available_cxp_columns.append(col)
                consolidated_df[f'cxp_{col.lower().replace(" ", "_").replace(".", "").replace("#", "number")}'] = np.nan
        
        st.write(f"📊 Columnas CXP que se procesarán: {available_cxp_columns}")
        
        matched_cxp = 0
        
        if 'Asignacion' in consolidated_df.columns:
            asignaciones = consolidated_df['Asignacion'].dropna().head(5).tolist()
            st.write(f"🔍 Ejemplos de Asignacion calculadas: {asignaciones}")
            
            for idx, row in consolidated_df.iterrows():
                asignacion = clean_id_aggressive(row.get('Asignacion', ''))
                
                if asignacion and asignacion in cxp_dict:
                    cxp_row = cxp_dict[asignacion]
                    matched_cxp += 1
                    
                    for col in available_cxp_columns:
                        col_name = f'cxp_{col.lower().replace(" ", "_").replace(".", "").replace("#", "number")}'
                        value = cxp_row.get(col)
                        # Limpiar valores numéricos CXP
                        if col in ['CO Aereo', 'Arancel', 'IVA', 'Handling', 'Dest. Delivery', 'Amt. Due', 'Goods Value']:
                            value = clean_numeric_value(value)
                        consolidated_df.loc[idx, col_name] = value
                    
                    if matched_cxp <= 5:
                        st.write(f"✅ CXP Match {matched_cxp}: Asignacion '{asignacion}' encontrada")
        
        st.success(f"✅ CXP procesado: {matched_cxp} matches por Asignacion")
    
    # PASO 6: Aplicar formatos básicos
    consolidated_df = apply_basic_formatting(consolidated_df)
    
    # PASO 7: Validación de duplicados
    st.info("🔍 Validando duplicados por order_id...")
    
    if 'order_id' in consolidated_df.columns:
        initial_count = len(consolidated_df)
        consolidated_df = consolidated_df.drop_duplicates(subset=['order_id'], keep='first')
        final_count = len(consolidated_df)
        
        if initial_count != final_count:
            removed_count = initial_count - final_count
            st.warning(f"⚠️ Se removieron {removed_count} registros duplicados por order_id")
        else:
            st.success("✅ No se encontraron duplicados por order_id")
    
    st.success(f"🎉 Consolidación completada: {len(consolidated_df)} registros finales")
    return consolidated_df

def prepare_record_for_db(record):
    """Prepara un registro para inserción en la base de datos"""
    integer_columns = ['system_number', 'quantity', 'iva', 'ica']
    
    numeric_columns = [
        'unit_price', 'declare_value', 'meli_fee', 'fuente', 
        'senders_cost', 'gross_amount', 'net_received_amount',
        'digital_verification', 'net_real_amount', 'logistic_weight_lbs',
        'logistics_fob', 'logistics_weight', 'logistics_length', 
        'logistics_width', 'logistics_height', 'logistics_insurance',
        'logistics_logistics', 'logistics_duties_prealert', 
        'logistics_duties_pay', 'logistics_duty_fee', 'logistics_saving',
        'logistics_total',
        'aditionals_quantity', 'aditionals_unitprice', 'aditionals_total',
        'cxp_co_aereo', 'cxp_arancel', 'cxp_iva', 'cxp_handling',
        'cxp_dest_delivery', 'cxp_amt_due', 'cxp_goods_value'
    ]
    
    cleaned_record = {}
    
    for key, value in record.items():
        if pd.isna(value) or value is None:
            cleaned_record[key] = None
        elif isinstance(value, (pd.Timestamp, datetime)):
            cleaned_record[key] = value.strftime('%Y-%m-%d') if hasattr(value, 'strftime') else str(value)
        elif key in integer_columns:
            try:
                clean_val = clean_numeric_value(value)
                if clean_val is not None:
                    cleaned_record[key] = int(float(clean_val))
                else:
                    cleaned_record[key] = 0
            except:
                cleaned_record[key] = 0
        elif key in numeric_columns:
            clean_val = clean_numeric_value(value)
            if clean_val is not None:
                cleaned_record[key] = clean_val
            else:
                cleaned_record[key] = None
        else:
            cleaned_record[key] = str(value)
    
    return cleaned_record

def insert_to_supabase(df):
    """Inserta los datos consolidados en Supabase con validación de duplicados"""
    try:
        st.info("🔍 Preparando datos para inserción...")
        
        df_mapped = map_column_names(df)
        
        db_columns = [
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
        
        # Agregar columnas adicionales si existen
        for col in df_mapped.columns:
            if (col.startswith('logistics_') or col.startswith('aditionals_') or col.startswith('cxp_')) and col not in db_columns:
                db_columns.append(col)
        
        df_filtered = df_mapped[[col for col in db_columns if col in df_mapped.columns]]
        
        st.info(f"📊 Preparando {len(df_filtered)} registros con {len(df_filtered.columns)} columnas")
        
        records = df_filtered.to_dict('records')
        
        # Limpiar registros
        st.info("🔧 Limpiando datos para base de datos...")
        cleaned_records = []
        for record in records:
            cleaned_record = prepare_record_for_db(record)
            cleaned_records.append(cleaned_record)
        
        # Verificar duplicados
        order_ids = [r.get('order_id') for r in cleaned_records if r.get('order_id')]
        if len(set(order_ids)) != len(order_ids):
            st.warning(f"⚠️ Detectados duplicados en order_id. Removiendo...")
            seen_order_ids = set()
            unique_records = []
            for record in cleaned_records:
                order_id = record.get('order_id')
                if order_id not in seen_order_ids:
                    seen_order_ids.add(order_id)
                    unique_records.append(record)
            cleaned_records = unique_records
            st.info(f"✅ Registros únicos para insertar: {len(cleaned_records)}")
        
        # Insertar en lotes
        batch_size = 50
        total_inserted = 0
        errors = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i in range(0, len(cleaned_records), batch_size):
            batch = cleaned_records[i:i + batch_size]
            
            try:
                result = supabase.table('consolidated_orders').insert(batch).execute()
                total_inserted += len(batch)
                
                progress = min(1.0, (i + batch_size) / len(cleaned_records))
                progress_bar.progress(progress)
                status_text.text(f"Insertando: {total_inserted}/{len(cleaned_records)} registros")
                
            except Exception as batch_error:
                error_msg = f"Error en lote {i//batch_size + 1}: {str(batch_error)}"
                st.error(error_msg)
                errors.append(error_msg)
                continue
        
        progress_bar.progress(1.0)
        status_text.text(f"✅ Completado: {total_inserted} registros insertados")
        
        # Log del procesamiento
        try:
            log_data = {
                'file_type': 'consolidated',
                'records_processed': len(cleaned_records),
                'records_matched': total_inserted,
                'status': 'success' if not errors else 'partial_success',
                'error_message': '; '.join(errors) if errors else None
            }
            supabase.table('processing_logs').insert(log_data).execute()
        except Exception as log_error:
            st.warning(f"Error logging process: {str(log_error)}")
        
        return total_inserted
        
    except Exception as e:
        st.error(f"Error general: {str(e)}")
        return 0

# INTERFAZ PRINCIPAL
def main():
    st.title("📦 Consolidador de Órdenes")
    st.markdown("### Procesa y consolida archivos con formatos profesionales")
    
    # Sidebar con información
    with st.sidebar:
        st.header("⚙️ Configuración")
        
        has_existing_data = check_existing_data()
        
        if has_existing_data:
            st.warning("⚠️ Hay datos existentes en la BD")
            clear_data = st.checkbox(
                "🗑️ Limpiar datos existentes antes de procesar",
                value=False,
                help="Marcar solo si desea eliminar TODOS los datos existentes"
            )
        else:
            st.success("✅ Base de datos limpia")
            clear_data = False
        
        st.info("💾 Los datos se guardarán automáticamente en la base de datos")
        
        st.markdown("---")
        st.markdown("**📋 Sistema Incremental:**")
        st.markdown("• Puedes subir archivos de forma independiente")
        st.markdown("• Cada archivo puede procesarse por separado")
        st.markdown("• Los datos se actualizan sin duplicar")
        
        st.markdown("---")
        st.markdown("**🎨 Formatos aplicados:**")
        st.markdown("• **Currency** sin decimales")
        st.markdown("• **Currency** con decimales")
        st.markdown("• **Fechas** formato estándar")
        st.markdown("• **Acentos** corregidos automáticamente")
        st.markdown("• **Valores basura** eliminados (XXXXXXXXXX)")
    
    # Área principal
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📁 Subir Archivos")
        
        drapify_file = st.file_uploader(
            "1. Archivo Drapify (Base de datos)",
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
        
        # Selector de fecha para Logistics
        if logistics_file:
            st.subheader("📅 Fecha para archivo Logistics")
            col_date1, col_date2, col_date3 = st.columns(3)
            
            with col_date1:
                if st.button("📅 Hoy"):
                    st.session_state.logistics_date = datetime.now().date()
            
            with col_date2:
                if st.button("📅 Ayer"):
                    st.session_state.logistics_date = (datetime.now() - timedelta(days=1)).date()
            
            with col_date3:
                selected_date = st.date_input(
                    "Fecha personalizada",
                    value=st.session_state.get('logistics_date', datetime.now().date())
                )
                st.session_state.logistics_date = selected_date
            
            st.info(f"📅 Fecha seleccionada: **{st.session_state.get('logistics_date', datetime.now().date())}**")
        
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
            "Drapify": "✅" if drapify_file else "⚪",
            "Logistics": "✅" if logistics_file else "⚪",
            "Aditionals": "✅" if aditionals_file else "⚪",
            "CXP": "✅" if cxp_file else "⚪"
        }
        
        for file_type, status in files_status.items():
            st.write(f"{status} {file_type}")
        
        st.markdown("---")
        
        if any([drapify_file, logistics_file, aditionals_file, cxp_file]):
            st.success("✅ Archivos listos para procesar")
        else:
            st.info("📤 Sube al menos un archivo")
    
    # Botón de procesamiento
    if st.button("🚀 Procesar Archivos", disabled=not any([drapify_file, logistics_file, aditionals_file, cxp_file]), type="primary"):
        
        with st.spinner("Procesando archivos..."):
            try:
                # Limpiar datos si se seleccionó
                if clear_data and has_existing_data:
                    st.info("🗑️ Limpiando datos existentes...")
                    if clear_existing_data():
                        st.success("✅ Datos existentes eliminados")
                    else:
                        st.warning("⚠️ No se pudieron eliminar completamente los datos existentes")
                
                # Leer archivos disponibles
                drapify_df = None
                if drapify_file:
                    if drapify_file.name.endswith('.csv'):
                        drapify_df = pd.read_csv(drapify_file)
                    else:
                        drapify_df = pd.read_excel(drapify_file)
                    st.success(f"✅ Drapify cargado: {len(drapify_df)} registros")
                
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
                
                # Si hay Drapify, hacer consolidación completa
                if drapify_df is not None:
                    logistics_date = st.session_state.get('logistics_date') if logistics_file else None
                    consolidated_df = process_files_according_to_rules(
                        drapify_df, logistics_df, aditionals_df, cxp_df, logistics_date
                    )
                    
                    # Mostrar preview
                    st.header("👀 Preview de Datos Consolidados")
                    st.dataframe(consolidated_df.head(10), use_container_width=True)
                    
                    # Mostrar estadísticas
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
                    
                    # Guardar en base de datos
                    st.header("💾 Guardando en Base de Datos")
                    
                    with st.spinner("Insertando datos en Supabase..."):
                        inserted_count = insert_to_supabase(consolidated_df)
                        
                        if inserted_count > 0:
                            st.success(f"🎉 ¡Procesamiento completado exitosamente!")
                            col1, col2 = st.columns(2)
                            with col1:
                                st.success(f"✅ {len(consolidated_df)} registros procesados")
                            with col2:
                                st.success(f"✅ {inserted_count} registros guardados en BD")
                            st.balloons()
                        else:
                            st.error("❌ Error guardando en la base de datos")
                    
                    # Opción de descarga
                    st.header("💾 Descargar Resultado")
                    
                    display_df = apply_display_formatting(consolidated_df)
                    
                    csv_buffer = io.StringIO()
                    display_df.to_csv(csv_buffer, index=False)
                    csv_data = csv_buffer.getvalue()
                    
                    st.download_button(
                        label="📥 Descargar CSV con Formatos Profesionales",
                        data=csv_data,
                        file_name=f"consolidated_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        type="secondary"
                    )
                else:
                    st.warning("⚠️ Se requiere al menos el archivo Drapify para procesar")
                
            except Exception as e:
                st.error(f"❌ Error procesando archivos: {str(e)}")
                st.exception(e)
    
    # Sección de consultas
    st.markdown("---")
    st.header("🔍 Consultar Datos Existentes")
    
    query_col1, query_col2 = st.columns(2)
    
    with query_col1:
        if st.button("📊 Ver Estadísticas Generales"):
            try:
                result = supabase.table('consolidated_orders').select('account_name').execute()
                
                if result.data:
                    df = pd.DataFrame(result.data)
                    if 'account_name' in df.columns:
                        st.subheader("Registros por Account Name")
                        account_counts = df['account_name'].value_counts()
                        st.bar_chart(account_counts)
                        st.dataframe(account_counts.reset_index())
                else:
                    st.info("No hay datos en la base de datos")
                    
            except Exception as e:
                st.error(f"Error consultando estadísticas: {str(e)}")
    
    with query_col2:
        if st.button("📋 Ver Últimos Registros"):
            try:
                result = supabase.table('consolidated_orders').select('*').order('id', desc=True).limit(10).execute()
                
                if result.data:
                    recent_df = pd.DataFrame(result.data)
                    st.subheader("Últimos 10 Registros")
                    st.dataframe(recent_df, use_container_width=True)
                else:
                    st.info("No hay datos en la base de datos")
                    
            except Exception as e:
                st.error(f"Error consultando registros: {str(e)}")
    
    # Búsqueda específica
    st.subheader("🔎 Búsqueda Específica")
    
    search_col1, search_col2, search_col3 = st.columns(3)
    
    with search_col1:
        search_order_id = st.text_input("Buscar por Order ID")
    
    with search_col2:
        search_prealert_id = st.text_input("Buscar por Prealert ID")
    
    with search_col3:
        search_account = st.selectbox(
            "Filtrar por Account",
            ["Todos", "1-TODOENCARGO-CO", "2-MEGATIENDA SPA", "3-VEENDELO", 
             "4-MEGA TIENDAS PERUANAS", "5-DETODOPARATODOS", "6-COMPRAFACIL", 
             "7-COMPRA-YA", "8-FABORCARGO"]
        )
    
    if st.button("🔍 Buscar"):
        try:
            query = supabase.table('consolidated_orders').select('*')
            
            if search_order_id:
                query = query.eq('order_id', search_order_id)
            
            if search_prealert_id:
                query = query.eq('prealert_id', search_prealert_id)
            
            if search_account != "Todos":
                query = query.eq('account_name', search_account)
            
            result = query.execute()
            
            if result.data:
                search_df = pd.DataFrame(result.data)
                st.success(f"✅ Encontrados {len(search_df)} registros")
                st.dataframe(search_df, use_container_width=True)
            else:
                st.warning("No se encontraron registros con los criterios especificados")
                
        except Exception as e:
            st.error(f"Error en la búsqueda: {str(e)}")

if __name__ == "__main__":
    main()
