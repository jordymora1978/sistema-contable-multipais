import streamlit as st
import pandas as pd
import numpy as np
from supabase import create_client, Client
import os
from datetime import datetime, date # Importar date también
import io
import time
import re
import traceback # Para mejor debugging

# --- Configuración de la página ---
st.set_page_config(
    page_title="Consolidador de Órdenes",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Configuración de Supabase con credenciales ---
# ATENCIÓN: Para producción, ¡usa Streamlit Secrets o variables de entorno!
# Por ejemplo:
# url = st.secrets["SUPABASE_URL"]
# key = st.secrets["SUPABASE_KEY"]
@st.cache_resource
def init_supabase():
    url = "https://pvbzzpeyhhxexyabizbv.supabase.co"
    key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB2Ynp6cGV5aGh4ZXh5YWJpemJ2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM5OTc5ODcsImV4cCI6MjA2OTU3Mzk4N30.06S8jDjNReAd6Oj8AZyO0PUcO2ASJHVA3VUNYVeAR4" # Asegúrate que esta key sea correcta
    return create_client(url, key)

supabase = init_supabase()

# Test de conexión al inicio
try:
    test_result = supabase.table('consolidated_orders').select('id').limit(1).execute()
    st.sidebar.success("✅ Conectado a Supabase")
except Exception as e:
    st.sidebar.error(f"❌ Error de conexión: {str(e)}")
    st.sidebar.info("Por favor, verifica tu URL y Key de Supabase.")


# --- FUNCIONES DE LIMPIEZA Y CONVERSIÓN DE DATOS (Mejoradas) ---

def clean_numeric_value(value, target_type='float'):
    """
    Limpia y convierte valores numéricos.
    target_type: 'integer' para int, 'float' para float.
    Retorna int, float o None.
    """
    if pd.isna(value) or value is None:
        return None
        
    str_val = str(value).strip()
    
    # Remover comilla simple inicial (común en CSVs de IDs grandes)
    if str_val.startswith("'"):
        str_val = str_val[1:]

    # Si está vacío o es un valor 'NaN' en texto, devolver None
    if not str_val or str_val.lower() in ['nan', 'none', '', '-']: # Añadido '-' como valor vacío
        return None
    
    cleaned_str = ''
    if target_type == 'integer':
        # Para enteros, solo queremos dígitos
        cleaned_str = ''.join(c for c in str_val if c.isdigit())
    else: # Para flotantes, permitimos dígitos y un punto decimal
        dot_count = 0
        for c in str_val:
            if c.isdigit():
                cleaned_str += c
            elif c == '.' and dot_count == 0:
                cleaned_str += c
                dot_count += 1
            # Ignorar otros caracteres como comas de miles o símbolos de moneda.
            # La conversión a float manejará los formatos numéricos estándar.
            
    if not cleaned_str or cleaned_str in ['.', '-.', '-']: # Evitar convertir solo un punto o guion
        return None

    try:
        # Primero convertir a float para manejar casos como "123.0"
        float_val = float(cleaned_str)
        if target_type == 'integer':
            # Si el valor es un entero exacto (ej. 123.0), convertir a int
            if float_val == int(float_val):
                return int(float_val)
            else:
                # Si tiene decimales reales (ej. 123.45), redondear y convertir a int
                return round(float_val) 
        else:
            return float_val
    except ValueError:
        # Si la conversión falla (ej. texto no numérico), devuelve None
        return None

def clean_datetime_value(value):
    """Limpia y convierte valores de fecha/hora para la BD a formato ISO (YYYY-MM-DD)"""
    if pd.isna(value) or value is None or str(value).strip() == '':
        return None
    
    # Si ya es un objeto datetime o date, convertir a string ISO
    if isinstance(value, (datetime, date, pd.Timestamp, np.datetime64)):
        try:
            # Asegurarse de que sea un objeto datetime para isoformat()
            if isinstance(value, np.datetime64):
                value = pd.Timestamp(value)
            return value.date().isoformat() # Solo la parte de la fecha
        except:
            return str(value) # Fallback a string si hay algún problema

    date_str = str(value).strip()

    try:
        # Intentar parsear con pandas to_datetime, que es más robusto
        # errors='coerce' convertirá valores no válidos a NaT (Not a Time)
        parsed_date = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(parsed_date):
            return parsed_date.date().isoformat() # Solo la parte de la fecha en formato YYYY-MM-DD
    except:
        pass # Fallback si pd.to_datetime falla inesperadamente
            
    return None # Si no se puede convertir a una fecha válida, devuelve None

def identify_column_types(df):
    """
    Identifica el tipo de dato esperado para cada columna basado en el esquema deseado de la BD.
    ESTA FUNCIÓN ASUME QUE HAS CAMBIADO LOS TIPOS EN SUPABASE.
    """
    
    # Columnas que son INT4 o INT8 en la base de datos (según tu esquema deseado)
    integer_columns = {
        'system_number', 'quantity', 'iva', 'ica', # Confirmadas como INT en DB
        'serial_number', 'pack_id', 'client_doc_id', 'street_number', # <-- Estas deben ser INT en DB
        'numero_de_documento', 'logistics_order_number', 'logistics_reference', 'logistics_guide_number' # <-- Estas deben ser INT en DB
    }
    
    # Columnas que son NUMERIC (flotantes) en la base de datos
    float_columns = {
        'unit_price', 'declare_value', 'meli_fee', 'fuente',
        'senders_cost', 'gross_amount', 'net_received_amount', 'net_real_amount',
        'logistic_weight_lbs', 'logistics_fob', 'logistics_weight', 'logistics_length',
        'logistics_width', 'logistics_height', 'logistics_insurance', 'logistics_logistics',
        'logistics_duties_prealert', 'logistics_duties_pay', 'logistics_duty_fee',
        'logistics_saving', 'logistics_total',
        'aditionals_unitprice', 'aditionals_total',
        'digital_verification',
        'cxp_co_aereo', 'cxp_arancel', 'cxp_iva', 'cxp_handling',
        'cxp_dest_delivery', 'cxp_amt_due', 'cxp_goods_value'
    }
    
    # Columnas que son TIMESTAMP en la base de datos
    datetime_columns = {
        'date_created', 'refunded_date', 'processing_date'
    }

    # Columnas que son TEXT en la base de datos (o JSONB)
    text_columns = {
        'order_id', 'asin', 'client_first_name', 'client_last_name', 'account_name',
        'title', 'logistic_type', 'address_line', 'street_name', 'city', 'state',
        'country', 'receiver_phone', 'amz_order_id', 'prealert_id', 'etiqueta_envio',
        'order_status_meli', 'nombre_del_tercero', 'direccion', 'apelido_del_tercero',
        'estado', 'razon_social', 'ciudad', 'tipo', 'telefono',
        'giro', 'correo', 'asignacion',
        'logistics_sap_code', 'logistics_invoice', 'logistics_status', 'logistics_unit',
        'logistics_description', 'logistics_shipper', 'logistics_phone', 'logistics_consignee',
        'logistics_identification', 'logistics_country', 'logistics_state', 'logistics_city',
        'logistics_address', 'logistics_master_guide', 'logistics_tariff_position',
        'logistics_external_id', 'aditionals_order_id', 'aditionals_item', 'aditionals_reference',
        'aditionals_description', 'cxp_ot_number', 'cxp_date', 'cxp_ref_number', 'cxp_consignee',
        'processing_notes', 'source_files', 'match_statistics' # JSONB se maneja como texto/dict en Python
    }
    
    # Asegúrate de que no haya solapamientos
    all_sets = [integer_columns, float_columns, datetime_columns, text_columns]
    for i, s1 in enumerate(all_sets):
        for j, s2 in enumerate(all_sets):
            if i != j:
                intersection = s1.intersection(s2)
                if intersection:
                    st.warning(f"⚠️ ¡Advertencia de solapamiento en identify_column_types! Columnas en múltiples categorías: {intersection}")

    return integer_columns, float_columns, datetime_columns, text_columns

def check_existing_data():
    """Verifica si hay datos existentes en la tabla"""
    try:
        result = supabase.table('consolidated_orders').select('id').limit(1).execute()
        return len(result.data) > 0
    except Exception as e:
        st.error(f"Error al verificar datos existentes: {str(e)}")
        return False

def clear_existing_data():
    """Elimina todos los registros existentes de las tablas"""
    try:
        # Intenta usar la función RPC si la tienes configurada en Supabase
        # Crea una función SQL en Supabase para truncar tablas:
        # CREATE OR REPLACE FUNCTION public.truncate_tables()
        # RETURNS void
        # LANGUAGE plpgsql
        # AS $function$
        # BEGIN
        #  TRUNCATE TABLE consolidated_orders RESTART IDENTITY CASCADE;
        #  TRUNCATE TABLE processing_logs RESTART IDENTITY CASCADE;
        # END;
        # $function$;
        # GRANT EXECUTE ON FUNCTION public.truncate_tables() TO anon;
        
        # O usa el método directo si la RPC no está configurada o falla
        supabase.table('consolidated_orders').delete().neq('id', 0).execute()
        supabase.table('processing_logs').delete().neq('id', 0).execute()
        return True
    except Exception as e:
        st.error(f"Error limpiando datos: {str(e)}")
        st.exception(e) # Muestra el traceback completo para debugging
        return False

def clean_id(value):
    """Limpia y normaliza IDs removiendo comillas y espacios, sin convertir a int."""
    if pd.isna(value):
        return None
    str_value = str(value).strip()
    # Remover comilla simple al inicio si existe
    if str_value.startswith("'"):
        str_value = str_value[1:]
    # Remover .0 al final si es un número entero (solo si es un string numérico)
    if str_value.endswith('.0') and str_value[:-2].isdigit():
        str_value = str_value[:-2]
    
    # Asegurarse de que no quede 'nan' como string
    return str_value if str_value and str_value.lower() != 'nan' else None

def calculate_asignacion(account_name, serial_number):
    """Calcula la asignación basada en el account_name y serial_number"""
    if pd.isna(account_name) or pd.isna(serial_number):
        return None
        
    # Limpiar serial_number - clean_id ahora devuelve string o None
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
        
        # Columnas de Logistics
        'Guide Number': 'logistics_guide_number',
        'Order number': 'logistics_order_number',
        'Reference': 'logistics_reference',
        'SAP Code': 'logistics_sap_code',
        'Invoice': 'logistics_invoice',
        'Status': 'logistics_status',
        'FOB': 'logistics_fob',
        'Unit': 'logistics_unit',
        'Weight': 'logistics_weight',
        'Length': 'logistics_length',
        'Width': 'logistics_width',
        'Height': 'logistics_height',
        'Insurance': 'logistics_insurance',
        'Logistics': 'logistics_logistics',
        'Duties Prealert': 'logistics_duties_prealert',
        'Duties Pay': 'logistics_duties_pay',
        'Duty Fee': 'logistics_duty_fee',
        'Saving': 'logistics_saving',
        'Total': 'logistics_total',
        'Description': 'logistics_description',
        'Shipper': 'logistics_shipper',
        'Phone': 'logistics_phone',
        'Consignee': 'logistics_consignee',
        'Identification': 'logistics_identification',
        'Country': 'logistics_country',
        'State': 'logistics_state',
        'City': 'logistics_city',
        'Address': 'logistics_address',
        'Master Guide': 'logistics_master_guide',
        'Tariff Position': 'logistics_tariff_position',
        'External Id': 'logistics_external_id',

        # Columnas de Aditionals
        'Order Id': 'aditionals_order_id',
        'Item': 'aditionals_item',
        'Reference': 'aditionals_reference',
        'Description': 'aditionals_description',
        'Quantity': 'aditionals_quantity',
        'UnitPrice': 'aditionals_unitprice',
        'Total': 'aditionals_total',

        # Columnas de CXP
        'OT Number': 'cxp_ot_number',
        'Date': 'cxp_date',
        'Ref #': 'cxp_ref_number',
        'Consignee': 'cxp_consignee',
        'CO Aereo': 'cxp_co_aereo',
        'Arancel': 'cxp_arancel',
        'IVA': 'cxp_iva',
        'Handling': 'cxp_handling',
        'Dest. Delivery': 'cxp_dest_delivery',
        'Amt. Due': 'cxp_amt_due',
        'Goods Value': 'cxp_goods_value'
    }
    
    renamed_df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    # También estandarizar los nombres de las columnas CXP que pueden venir con espacios adicionales
    # This logic was already in process_files_according_to_rules. Moving it here for consistency
    # and ensuring all columns are mapped cleanly.
    cxp_cols_to_clean = {
        ' ot_number ': 'cxp_ot_number',
        ' date ': 'cxp_date',
        ' ref_number ': 'cxp_ref_number', # Assuming Ref # gets mapped to cxp_ref_number
        ' consignee ': 'cxp_consignee',
        ' co_aereo ': 'cxp_co_aereo',
        ' arancel ': 'cxp_arancel',
        ' iva ': 'cxp_iva',
        ' handling ': 'cxp_handling',
        ' dest_delivery ': 'cxp_dest_delivery',
        ' amt_due ': 'cxp_amt_due',
        ' goods_value ': 'cxp_goods_value'
    }
    renamed_df = renamed_df.rename(columns={k: v for k, v in cxp_cols_to_clean.items() if k in renamed_df.columns})

    return renamed_df

def apply_basic_formatting(df):
    """Aplica formatos básicos sin afectar campos numéricos para BD"""
    
    st.info("🔧 Aplicando formatos básicos para base de datos (encoding y fechas)...")
    
    # C) Corregir encoding en columnas de texto (usa la lista de identify_column_types)
    # Temporalmente, define una lista de columnas de texto para el encoding si no tienes la DB actualizada todavía.
    # Idealmente, identify_column_types se usaría aquí también.
    text_cols_for_encoding = [
        'client_first_name', 'client_last_name', 'title', 'address_line', 
        'street_name', 'city', 'state', 'country', 'nombre_del_tercero',
        'direccion', 'apelido_del_tercero', 'estado', 'razon_social', 'ciudad',
        'logistics_description', 'logistics_shipper', 'logistics_consignee',
        'logistics_country', 'logistics_state', 'logistics_city', 'logistics_address',
        'order_id', 'asin', 'prealert_id', 'etiqueta_envio', 'order_status_meli',
        'logistic_type', 'amz_order_id', 'receiver_phone', 'tipo', 'telefono', 'giro', 'correo',
        'logistics_sap_code', 'logistics_invoice', 'logistics_status', 'logistics_unit',
        'logistics_master_guide', 'logistics_tariff_position', 'logistics_external_id',
        'aditionals_order_id', 'aditionals_item', 'aditionals_reference', 'aditionals_description',
        'cxp_ot_number', 'cxp_date', 'cxp_ref_number', 'cxp_consignee', 'asignacion'
    ]
    
    for col in text_cols_for_encoding:
        if col in df.columns:
            df[col] = df[col].apply(fix_encoding)
    
    st.success("✅ Formatos básicos aplicados")
    return df

def apply_display_formatting(df):
    """Aplica formatos de visualización (currency) solo para descarga CSV"""
    
    st.info("🎨 Aplicando formatos de visualización para descarga...")
    
    display_df = df.copy()
    
    currency_no_decimals_columns = [
        'unit_price', 'meli_fee', 'iva', 'ica', 'fuente', 
        'senders_cost', 'gross_amount', 'net_received_amount', 'net_real_amount',
        # 'order_cost', 'Meli Fee', 'IVA', 'ICA', 'FUENTE' # Estas son columnas originales, no finales
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
        'aditionals_unitprice', 'aditionals_total',
        'digital_verification',
        'cxp_co_aereo', 'cxp_arancel', 'cxp_iva', 'cxp_handling', 
        'cxp_dest_delivery', 'cxp_amt_due', 'cxp_goods_value'
    ]
    
    for col in currency_with_decimals_columns:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(format_currency_with_decimals)
            
    # Formatear fechas para display (solo las de tipo datetime_columns)
    # Si 'cxp_date' es TEXT en la DB, no la formates aquí como fecha
    datetime_cols_for_display = ['date_created', 'refunded_date', 'processing_date']
    for col in datetime_cols_for_display:
        if col in display_df.columns:
            # pd.to_datetime con errors='coerce' maneja bien NaNs
            display_df[col] = pd.to_datetime(display_df[col], errors='coerce').dt.strftime('%Y-%m-%d')
            display_df[col] = display_df[col].replace({pd.NA: None, np.nan: None}) # Limpiar NaT a None
            
    st.success("✅ Formatos de visualización aplicados")
    return display_df

def process_files_according_to_rules(drapify_df, logistics_df=None, aditionals_df=None, cxp_df=None):
    """
    Procesa y consolida todos los archivos según las reglas especificadas.
    """
    st.info("🔄 Iniciando consolidación según reglas especificadas...")
    
    # PASO 1: Usar Drapify como base
    consolidated_df = drapify_df.copy()
    st.success(f"✅ Archivo base Drapify procesado: {len(consolidated_df)} registros")
    
    # PASO 2: Procesar archivo Logistics
    if logistics_df is not None and not logistics_df.empty:
        st.info("🚚 Procesando archivo Logistics...")
        
        logistics_dict_by_reference = {clean_id(row.get('Reference', '')): row for idx, row in logistics_df.iterrows() if clean_id(row.get('Reference', ''))}
        logistics_dict_by_order_number = {clean_id(row.get('Order number', '')): row for idx, row in logistics_df.iterrows() if clean_id(row.get('Order number', ''))}
        
        st.info(f"📋 Logistics indexado: {len(logistics_dict_by_reference)} por Reference, {len(logistics_dict_by_order_number)} por Order number")
        
        # Columnas de Logistics a añadir, según tu mapeo
        logistics_columns_to_add = [
            'Guide Number', 'Order number', 'Reference', 'SAP Code', 'Invoice', 'Status', 'FOB', 'Unit',
            'Weight', 'Length', 'Width', 'Height', 'Insurance', 'Logistics', 'Duties Prealert',
            'Duties Pay', 'Duty Fee', 'Saving', 'Total', 'Description', 'Shipper', 'Phone',
            'Consignee', 'Identification', 'Country', 'State', 'City', 'CAddress', # Ojo: "Address" en source, "CAddress" en db?
            'Master Guide', 'Tariff Position', 'External Id' # 'Invoice' repetida ya fue mapeada
        ]
        
        # Inicializar columnas de Logistics con NaN
        for col_orig in logistics_columns_to_add:
            col_db_name = f'logistics_{col_orig.lower().replace(" ", "_").replace("#", "number")}'
            # Solo añadir si la columna mapeada existe en la DB_columns de la función de inserción
            # Esto es un poco hacky, idealmente tendrías un esquema único aquí.
            # Por ahora, nos aseguramos de que no cree columnas "fantasma".
            # Las columnas CXP y adicionales se inicializan más abajo
            if col_db_name not in consolidated_df.columns: # Evitar duplicados si ya existen
                consolidated_df[col_db_name] = np.nan
        
        matched_by_order_id = 0
        matched_by_prealert_id = 0
        
        for idx, row in consolidated_df.iterrows():
            order_id = clean_id(row.get('order_id', ''))
            prealert_id = clean_id(row.get('prealert_id', ''))
            
            logistics_row = None
            match_type = None
            
            if order_id and order_id in logistics_dict_by_reference:
                logistics_row = logistics_dict_by_reference[order_id]
                matched_by_order_id += 1
                match_type = "order_id->Reference"
            elif prealert_id and prealert_id in logistics_dict_by_order_number:
                logistics_row = logistics_dict_by_order_number[prealert_id]
                matched_by_prealert_id += 1
                match_type = "prealert_id->Order number"
                
            if logistics_row is not None:
                for col_orig in logistics_columns_to_add:
                    if col_orig in logistics_df.columns:
                        col_db_name = f'logistics_{col_orig.lower().replace(" ", "_").replace("#", "number")}'
                        consolidated_df.loc[idx, col_db_name] = logistics_row.get(col_orig)
        
        st.success(f"✅ Logistics procesado: {matched_by_order_id} matches por order_id, {matched_by_prealert_id} matches por prealert_id")
    
    # PASO 3: Procesar archivo Aditionals
    if aditionals_df is not None and not aditionals_df.empty:
        st.info("➕ Procesando archivo Aditionals...")
        
        aditionals_dict = {clean_id(row.get('Order Id', '')): row for idx, row in aditionals_df.iterrows() if clean_id(row.get('Order Id', ''))}
        
        st.info(f"📋 Aditionals indexado: {len(aditionals_dict)} registros")
        
        aditionals_columns_to_add = ['Order Id', 'Item', 'Reference', 'Description', 'Quantity', 'UnitPrice', 'Total']
        
        for col_orig in aditionals_columns_to_add:
            col_db_name = f'aditionals_{col_orig.lower().replace(" ", "_")}'
            if col_db_name not in consolidated_df.columns:
                consolidated_df[col_db_name] = np.nan
        
        matched_aditionals = 0
        
        for idx, row in consolidated_df.iterrows():
            prealert_id = clean_id(row.get('prealert_id', ''))
            
            if prealert_id and prealert_id in aditionals_dict:
                aditionals_row = aditionals_dict[prealert_id]
                matched_aditionals += 1
                
                for col_orig in aditionals_columns_to_add:
                    if col_orig in aditionals_df.columns:
                        col_db_name = f'aditionals_{col_orig.lower().replace(" ", "_")}'
                        consolidated_df.loc[idx, col_db_name] = aditionals_row.get(col_orig)
        
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
        
        # Mapeo de columnas CXP: Mover esta lógica a map_column_names para mejor modularidad.
        # Por ahora, solo confirmamos las columnas disponibles aquí.
        
        # Crear diccionario para mapeo rápido de CXP
        cxp_dict = {}
        # Primero, normalizamos las columnas de CXP para el diccionario
        cxp_df_normalized_for_dict = cxp_df.rename(columns={
            'ot_number': 'OT Number', 'date': 'Date', 'consignee': 'Consignee', 
            'co_aereo': 'CO Aereo', 'arancel': 'Arancel', 'iva': 'IVA',
            'dest_delivery': 'Dest. Delivery', ' Amt. Due ': 'Amt. Due', ' Goods Value ': 'Goods Value',
            'Ref #': 'Ref #' # Asegurar que esta también se considere
        })

        for idx, row in cxp_df_normalized_for_dict.iterrows():
            ref_number = clean_id(row.get('Ref #', '')) # 'Ref #' es el nombre del archivo, 'cxp_ref_number' en DB
            if ref_number:
                cxp_dict[ref_number] = row
        
        st.info(f"📋 CXP indexado: {len(cxp_dict)} registros")
        
        cxp_refs = list(cxp_dict.keys())[:5]
        st.write(f"🔍 Ejemplos de Ref # en CXP: {cxp_refs}")
        
        standard_cxp_columns = [
            'OT Number', 'Date', 'Ref #', 'Consignee', 'CO Aereo', 
            'Arancel', 'IVA', 'Handling', 'Dest. Delivery', 'Amt. Due', 'Goods Value'
        ]
        
        # Inicializar columnas de CXP en consolidated_df
        for col_orig in standard_cxp_columns:
            col_db_name = f'cxp_{col_orig.lower().replace(" ", "_").replace(".", "").replace("#", "number")}'
            if col_db_name not in consolidated_df.columns:
                consolidated_df[col_db_name] = np.nan
        
        matched_cxp = 0
        
        if 'asignacion' in consolidated_df.columns: # Usar el nombre de columna mapeado
            asignaciones = consolidated_df['asignacion'].dropna().head(5).tolist()
            st.write(f"🔍 Ejemplos de Asignacion calculadas: {asignaciones}")
            
            for idx, row in consolidated_df.iterrows():
                asignacion = clean_id(row.get('asignacion', ''))
                
                if asignacion and asignacion in cxp_dict:
                    cxp_row = cxp_dict[asignacion]
                    matched_cxp += 1
                    
                    for col_orig in standard_cxp_columns:
                        col_db_name = f'cxp_{col_orig.lower().replace(" ", "_").replace(".", "").replace("#", "number")}'
                        consolidated_df.loc[idx, col_db_name] = cxp_row.get(col_orig)
        
        st.success(f"✅ CXP procesado: {matched_cxp} matches por Asignacion")
    
    # PASO 6: Aplicar formatos básicos (encoding y fechas)
    consolidated_df = apply_basic_formatting(consolidated_df)
    
    # PASO 7: Validación de duplicados por order_id
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

# --- Función para insertar datos en Supabase (Mejorada) ---
def insert_to_supabase(df):
    """Inserta los datos consolidados en Supabase con limpieza de tipos y validación."""
    try:
        st.info("🔍 Preparando datos para inserción...")
        
        # Mapear nombres de columnas del CSV a los nombres de la base de datos
        df_mapped = map_column_names(df)
        
        # Identificar tipos de columnas usando la función centralizada
        integer_cols, float_cols, datetime_cols, text_cols = identify_column_types(df_mapped) # Usa df_mapped aquí
        
        # Construir la lista final de columnas a insertar, asegurando que estén en el orden correcto si es necesario
        # Y solo las que existen en el DataFrame mapeado
        final_db_columns_order = [
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
            'asignacion',
            # Columnas logistics
            'logistics_guide_number', 'logistics_order_number', 'logistics_reference',
            'logistics_sap_code', 'logistics_invoice', 'logistics_status', 'logistics_fob',
            'logistics_unit', 'logistics_weight', 'logistics_length', 'logistics_width',
            'logistics_height', 'logistics_insurance', 'logistics_logistics',
            'logistics_duties_prealert', 'logistics_duties_pay', 'logistics_duty_fee',
            'logistics_saving', 'logistics_total', 'logistics_description', 'logistics_shipper',
            'logistics_phone', 'logistics_consignee', 'logistics_identification',
            'logistics_country', 'logistics_state', 'logistics_city', 'logistics_address',
            'logistics_master_guide', 'logistics_tariff_position', 'logistics_external_id',
            # Columnas aditionals
            'aditionals_order_id', 'aditionals_item', 'aditionals_reference',
            'aditionals_description', 'aditionals_quantity', 'aditionals_unitprice', 'aditionals_total',
            # Columnas cxp
            'cxp_ot_number', 'cxp_date', 'cxp_ref_number', 'cxp_consignee', 'cxp_co_aereo',
            'cxp_arancel', 'cxp_iva', 'cxp_handling', 'cxp_dest_delivery', 'cxp_amt_due', 'cxp_goods_value',
            # Columnas de metadata (siempre se añaden si existen)
            'processing_date', 'source_files', 'match_statistics', 'processing_notes'
        ]

        df_filtered = df_mapped[[col for col in final_db_columns_order if col in df_mapped.columns]].copy()
        
        st.info(f"📊 Preparando {len(df_filtered)} registros con {len(df_filtered.columns)} columnas")
        
        st.info("🧹 Limpiando tipos de datos para inserción en BD...")
        
        # Aplicar limpieza de tipos para cada columna
        for col in df_filtered.columns:
            if col in integer_cols:
                df_filtered[col] = df_filtered[col].apply(lambda x: clean_numeric_value(x, 'integer'))
            elif col in float_cols:
                df_filtered[col] = df_filtered[col].apply(lambda x: clean_numeric_value(x, 'float'))
            elif col in datetime_cols:
                df_filtered[col] = df_filtered[col].apply(clean_datetime_value)
            elif col in text_cols: # Columnas de texto, asegurar que sean string o None
                df_filtered[col] = df_filtered[col].apply(lambda x: str(x) if pd.notna(x) and x is not None and str(x).strip() != '' else None)
            else: # Columna no mapeada, default a string
                st.warning(f"Columna '{col}' no categorizada en identify_column_types. Se tratará como texto.")
                df_filtered[col] = df_filtered[col].apply(lambda x: str(x) if pd.notna(x) and x is not None and str(x).strip() != '' else None)

        # Verificación final de tipos de datos antes de to_dict('records')
        st.info("🔍 Verificando tipos finales de columnas clave para depuración...")
        debug_cols = ['system_number', 'serial_number', 'order_id', 'pack_id', 'client_doc_id', 'date_created', 'unit_price', 'logistics_guide_number', 'cxp_co_aereo']
        for col in debug_cols:
            if col in df_filtered.columns:
                unique_types = df_filtered[col].apply(lambda x: type(x).__name__ if pd.notna(x) else 'NoneType').unique()
                sample_values = df_filtered[col].dropna().head(3).tolist()
                st.write(f"  **{col}**: Tipos únicos: {list(unique_types)}, Muestra: {sample_values}")
        
        st.success("✅ Limpieza de tipos para BD completada")
        
        # Convertir a lista de diccionarios para Supabase
        records = df_filtered.to_dict('records')
        
        # Añadir metadata de procesamiento (fecha actual, etc.)
        current_time_utc = datetime.utcnow().isoformat() + 'Z' # Formato ISO 8601 con Z para UTC
        for record in records:
            record['processing_date'] = current_time_utc
            # Puedes añadir más metadata si es necesario
            record['source_files'] = record.get('source_files', {}) # Asegurar que existe si no viene de los CSVs
            record['match_statistics'] = record.get('match_statistics', {})

        # Verificación adicional de duplicados por order_id (ya en tus reglas)
        order_ids_in_records = [r.get('order_id') for r in records if r.get('order_id')]
        if len(set(order_ids_in_records)) != len(order_ids_in_records):
            st.warning(f"⚠️ Detectados duplicados en order_id *después* de preparación. Removiendo para inserción...")
            seen_order_ids = set()
            unique_records = []
            for record in records:
                order_id = record.get('order_id')
                if order_id not in seen_order_ids:
                    seen_order_ids.add(order_id)
                    unique_records.append(record)
            records = unique_records
            st.info(f"✅ Registros únicos para insertar: {len(records)}")
            
        # DEBUG: Mostrar el primer registro después de toda la limpieza
        if records:
            st.write("🔍 DEBUG - Primer registro final para inserción:")
            first_record = records[0]
            for key, value in list(first_record.items())[:15]: # Mostrar primeros 15 campos
                st.write(f"  {key}: {value} (tipo: {type(value).__name__})")
        
        # Insertar en lotes
        batch_size = 100 # Aumentado a 100 para un mejor rendimiento
        total_inserted = 0
        errors = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            status_text.text(f"📦 Procesando lote {batch_num}: {len(batch)} registros")
            
            try:
                # La inserción se hace con los tipos ya limpios
                result = supabase.table('consolidated_orders').insert(batch).execute()
                total_inserted += len(batch)
                
                progress = min(1.0, (i + batch_size) / len(records))
                progress_bar.progress(progress)
                
            except Exception as batch_error:
                error_msg = f"Error en lote {batch_num}: {str(batch_error)}"
                st.error(error_msg)
                errors.append(error_msg)
                
                # Mostrar el primer registro del lote con error para depuración
                if batch:
                    st.write("🔍 Primer registro del lote con error:")
                    for key, value in list(batch[0].items())[:15]: # Solo primeros 15 campos
                        st.write(f"  {key}: {value} (tipo: {type(value).__name__})")
                
                continue # Continuar con el siguiente lote incluso si este falló
        
        progress_bar.progress(1.0)
        status_text.text(f"✅ Completado: {total_inserted} registros insertados")
        
        # Log del procesamiento
        try:
            log_data = {
                'file_type': 'consolidated',
                'records_processed': len(records),
                'records_matched': total_inserted,
                'status': 'success' if not errors else 'partial_success',
                'error_message': '; '.join(errors) if errors else None
            }
            supabase.table('processing_logs').insert(log_data).execute()
        except Exception as log_error:
            st.warning(f"Error logging process: {str(log_error)}")
            
        return total_inserted
        
    except Exception as e:
        st.error(f"❌ Error general en insert_to_supabase: {str(e)}")
        st.exception(e) # Muestra el traceback completo para debugging
        return 0

# --- Interfaz principal de Streamlit ---
def main():
    st.title("📦 Consolidador de Órdenes")
    st.markdown("### Procesa y consolida archivos con formatos profesionales")
    
    with st.sidebar:
        st.header("⚙️ Configuración")
        
        has_existing_data = check_existing_data()
        
        if has_existing_data:
            st.warning("⚠️ Hay datos existentes en la BD")
            clear_data = st.checkbox(
                "🗑️ Limpiar datos existentes antes de procesar",
                value=True,
                help="Recomendado para evitar duplicados y aplicar nuevos formatos. ¡Borrará TODOS los datos de las tablas 'consolidated_orders' y 'processing_logs'!"
            )
        else:
            st.success("✅ Base de datos limpia")
            clear_data = False
        
        st.info("💾 Los datos se guardarán automáticamente en la base de datos")
        
        st.markdown("---")
        st.markdown("**📋 Procesamiento mejorado:**")
        st.markdown("1. 📋 **Drapify** (base - obligatorio)")
        st.markdown("2. 🚚 **Logistics** (opcional)")
        st.markdown("3. ➕ **Aditionals** (opcional)")
        st.markdown("4. 🏷️ **Calcular Asignacion**")
        st.markdown("5. 💰 **CXP** (opcional)")
        st.markdown("6. 🎨 **Aplicar formatos profesionales** (para BD y descarga)")
        st.markdown("7. 🔍 **Validar duplicados**")
        st.markdown("8. 💾 **Guardar en Base de Datos**")
        
        st.markdown("---")
        st.markdown("**🎨 Formatos aplicados:**")
        st.markdown("• **Números** (enteros y decimales) a tipos correctos")
        st.markdown("• **Fechas** formato `YYYY-MM-DD`")
        st.markdown("• **Acentos** corregidos automáticamente")
        st.markdown("• **Duplicados** eliminados por `order_id`")
        
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
        
        st.markdown("---")
        
        if drapify_file:
            st.success("✅ Listo para procesar")
        else:
            st.warning("⚠️ Archivo Drapify requerido")
    
    if st.button("🚀 Procesar con Formatos Profesionales", disabled=not drapify_file, type="primary"):
        
        with st.spinner("Procesando archivos con formatos profesionales..."):
            try:
                if clear_data and has_existing_data:
                    st.info("🗑️ Limpiando datos existentes...")
                    if clear_existing_data():
                        st.success("✅ Datos existentes eliminados")
                    else:
                        st.error("❌ No se pudieron eliminar completamente los datos existentes. Intenta manualmente en Supabase si el problema persiste.")
                        st.stop() # Detiene la ejecución si la limpieza falla críticamente

                if drapify_file.name.endswith('.csv'):
                    drapify_df = pd.read_csv(drapify_file)
                else:
                    drapify_df = pd.read_excel(drapify_file)
                
                st.success(f"✅ Drapify cargado: {len(drapify_df)} registros")
                
                with st.expander("🔍 Columnas encontradas en Drapify"):
                    st.write(list(drapify_df.columns))
                
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
                
                consolidated_df = process_files_according_to_rules(
                    drapify_df, logistics_df, aditionals_df, cxp_df
                )
                
                st.header("👀 Preview de Datos Consolidados con Formatos")
                st.dataframe(consolidated_df.head(10), use_container_width=True)
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Registros", len(consolidated_df))
                
                with col2:
                    logistics_matched = 0
                    log_id_cols = ['logistics_guide_number', 'logistics_order_number', 'logistics_reference']
                    # Contar matches si alguna de las columnas clave de logistics tiene datos
                    if any(col in consolidated_df.columns for col in log_id_cols):
                        logistics_matched = consolidated_df[[c for c in log_id_cols if c in consolidated_df.columns]].notna().any(axis=1).sum()
                    st.metric("Logistics Matched", logistics_matched)
                
                with col3:
                    aditionals_matched = 0
                    if 'aditionals_order_id' in consolidated_df.columns:
                        aditionals_matched = consolidated_df['aditionals_order_id'].notna().sum()
                    st.metric("Aditionals Matched", aditionals_matched)
                
                with col4:
                    cxp_matched = 0
                    if 'cxp_ref_number' in consolidated_df.columns:
                        cxp_matched = consolidated_df['cxp_ref_number'].notna().sum()
                    st.metric("CXP Matched", cxp_matched)
                
                if 'asignacion' in consolidated_df.columns:
                    st.subheader("🏷️ Análisis de Asignaciones")
                    asignacion_counts = consolidated_df['asignacion'].value_counts().head(10)
                    st.bar_chart(asignacion_counts)
                
                st.header("💾 Guardando en Base de Datos")
                
                with st.spinner("Insertando datos con formatos profesionales en Supabase..."):
                    inserted_count = insert_to_supabase(consolidated_df)
                    
                    if inserted_count > 0:
                        st.success(f"🎉 ¡Procesamiento completado exitosamente!")
                        c1, c2 = st.columns(2)
                        with c1:
                            st.success(f"✅ {len(consolidated_df)} registros procesados")
                        with c2:
                            st.success(f"✅ {inserted_count} registros guardados en BD")
                        st.balloons()
                    else:
                        st.error("❌ Error guardando en la base de datos")
                        st.warning("Los datos fueron procesados correctamente pero no se pudieron guardar.")
                
                st.header("💾 Descargar Resultado")
                
                display_df = apply_display_formatting(consolidated_df)
                
                csv_buffer = io.StringIO()
                display_df.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()
                
                st.download_button(
                    label="📥 Descargar CSV con Formatos Profesionales",
                    data=csv_data,
                    file_name=f"consolidated_orders_formatted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    type="secondary"
                )
                
            except Exception as e:
                st.error(f"❌ Error procesando archivos: {str(e)}")
                st.exception(e)
    
    st.markdown("---")
    st.header("🔍 Consultar Datos Existentes")
    
    query_col1, query_col2 = st.columns(2)
    
    with query_col1:
        if st.button("📊 Ver Estadísticas Generales de Cuentas"):
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
                        st.info("Datos encontrados pero sin columna account_name.")
                else:
                    st.info("No hay datos en la base de datos.")
                    
            except Exception as e:
                st.error(f"Error consultando estadísticas: {str(e)}")
    
    with query_col2:
        if st.button("📋 Ver Últimos 10 Registros"):
            try:
                result = supabase.table('consolidated_orders').select('*').order('id', desc=True).limit(10).execute()
                
                if result.data:
                    recent_df = pd.DataFrame(result.data)
                    st.subheader("Últimos 10 Registros")
                    st.dataframe(recent_df, use_container_width=True)
                else:
                    st.info("No hay datos en la base de datos.")
                    
            except Exception as e:
                st.error(f"Error consultando registros: {str(e)}")
    
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
    
    if st.button("🔍 Buscar en BD"):
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
                st.warning("No se encontraron registros con los criterios especificados.")
                
        except Exception as e:
            st.error(f"Error en la búsqueda: {str(e)}")

if __name__ == "__main__":
    main()
