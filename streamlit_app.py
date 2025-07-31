import streamlit as st
import pandas as pd
import numpy as np
from supabase import create_client, Client
import os
from datetime import datetime
import io
import time

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
    # Configuración del nuevo proyecto Supabase
    url = "https://pvbzzpeyhhxexyabizbv.supabase.co"
    key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB2Ynp6cGV5aGh4ZXh5YWJpemJ2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM5OTc5ODcsImV4cCI6MjA2OTU3Mzk4N30.06S8jDjNReAd6Oj8AZvOS2PUcO2ASJHVA3VUNYVeAR4"
    return create_client(url, key)

supabase = init_supabase()

# Test de conexión al inicio
try:
    # Verificar conexión con la nueva tabla
    test_result = supabase.table('consolidated_orders').select('id').limit(1).execute()
    st.sidebar.success("✅ Conectado a Supabase")
except Exception as e:
    st.sidebar.error(f"❌ Error de conexión: {str(e)}")

# Función para limpiar y normalizar IDs
def clean_id(value):
    """Limpia y normaliza IDs removiendo comillas y espacios"""
    if pd.isna(value):
        return None
    str_value = str(value).strip()
    # Remover comilla simple al inicio si existe
    if str_value.startswith("'"):
        str_value = str_value[1:]
    # Remover .0 al final si es un número entero
    if str_value.endswith('.0'):
        str_value = str_value[:-2]
    return str_value if str_value and str_value != 'nan' else None

# Función para calcular asignación según las reglas especificadas
def calculate_asignacion(account_name, serial_number):
    """Calcula la asignación basada en el account_name y serial_number"""
    if pd.isna(account_name) or pd.isna(serial_number):
        return None
    
    # Limpiar serial_number para evitar decimales
    clean_serial = clean_id(serial_number)
    if not clean_serial:
        return None
    
    # Mapeo exacto según las especificaciones
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

# Función para mapear nombres de columnas del CSV a la base de datos
def map_column_names(df):
    """Mapea nombres de columnas del CSV a los nombres de la base de datos"""
    column_mapping = {
        # Columnas del sistema (se manejan automáticamente)
        # Columnas de Drapify
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
        
        # Columnas de Logistics (ya tienen prefijo logistics_)
        # Se mantienen como están
        
        # Columnas de Aditionals (ya tienen prefijo aditionals_)
        # Se mantienen como están
        
        # Asignacion
        'Asignacion': 'asignacion',
        
        # Columnas de CXP (ya tienen prefijo cxp_)
        # Se mantienen como están
    }
    
    # Aplicar mapeo solo a las columnas que existen
    renamed_df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    return renamed_df

# Función principal para procesar archivos según las reglas especificadas
def process_files_according_to_rules(drapify_df, logistics_df=None, aditionals_df=None, cxp_df=None):
    """
    Procesa y consolida todos los archivos según las reglas exactas especificadas:
    1. Drapify como base
    2. Logistics: buscar order_id en Reference, luego prealert_id en Order number
    3. Aditionals: buscar prealert_id en Order Id
    4. Calcular Asignacion
    5. CXP: buscar Asignacion en Ref #
    """
    
    st.info("🔄 Iniciando consolidación según reglas especificadas...")
    
    # PASO 1: Usar Drapify como base (todas las columnas tal como están)
    consolidated_df = drapify_df.copy()
    st.success(f"✅ Archivo base Drapify procesado: {len(consolidated_df)} registros")
    
    # PASO 2: Procesar archivo Logistics si está disponible
    if logistics_df is not None and not logistics_df.empty:
        st.info("🚚 Procesando archivo Logistics...")
        
        # Crear diccionario para mapeo rápido de Logistics
        logistics_dict_by_reference = {}
        logistics_dict_by_order_number = {}
        
        for idx, row in logistics_df.iterrows():
            # Limpiar los IDs para mejor matching
            reference = clean_id(row.get('Reference', ''))
            order_number = clean_id(row.get('Order number', ''))
            
            if reference:
                logistics_dict_by_reference[reference] = row
            if order_number:
                logistics_dict_by_order_number[order_number] = row
        
        st.info(f"📋 Logistics indexado: {len(logistics_dict_by_reference)} por Reference, {len(logistics_dict_by_order_number)} por Order number")
        
        # Agregar columnas de Logistics al DataFrame consolidado
        logistics_columns = [
            'Guide Number', 'Order number', 'Reference', 'SAP Code', 'Invoice', 
            'Status', 'FOB', 'Unit', 'Weight', 'Length', 'Width', 'Height',
            'Insurance', 'Logistics', 'Duties Prealert', 'Duties Pay', 
            'Duty Fee', 'Saving', 'Total', 'Description', 'Shipper', 'Phone',
            'Consignee', 'Identification', 'Country', 'State', 'City', 
            'Address', 'Master Guide', 'Tariff Position', 'External Id', 'Invoice'
        ]
        
        # Inicializar columnas de Logistics con NaN
        for col in logistics_columns:
            if col in logistics_df.columns:
                consolidated_df[f'logistics_{col.lower().replace(" ", "_")}'] = np.nan
        
        matched_by_order_id = 0
        matched_by_prealert_id = 0
        
        # Hacer el matching según las reglas
        for idx, row in consolidated_df.iterrows():
            # Limpiar los IDs para mejor matching
            order_id = clean_id(row.get('order_id', ''))
            prealert_id = clean_id(row.get('prealert_id', ''))
            
            logistics_row = None
            match_type = None
            
            # Regla 1: Buscar order_id en Reference
            if order_id and order_id in logistics_dict_by_reference:
                logistics_row = logistics_dict_by_reference[order_id]
                matched_by_order_id += 1
                match_type = "order_id->Reference"
            
            # Regla 2: Si no encuentra, buscar prealert_id en Order number
            elif prealert_id and prealert_id in logistics_dict_by_order_number:
                logistics_row = logistics_dict_by_order_number[prealert_id]
                matched_by_prealert_id += 1
                match_type = "prealert_id->Order number"
            
            # Si encontró match, copiar los datos
            if logistics_row is not None:
                for col in logistics_columns:
                    if col in logistics_df.columns:
                        consolidated_df.loc[idx, f'logistics_{col.lower().replace(" ", "_")}'] = logistics_row.get(col)
                
                # Debug: mostrar algunos matches
                if (matched_by_order_id + matched_by_prealert_id) <= 5:
                    st.write(f"✅ Match {matched_by_order_id + matched_by_prealert_id}: {match_type} - order_id: {order_id}, prealert_id: {prealert_id}")
        
        st.success(f"✅ Logistics procesado: {matched_by_order_id} matches por order_id, {matched_by_prealert_id} matches por prealert_id")
    
    # PASO 3: Procesar archivo Aditionals si está disponible
    if aditionals_df is not None and not aditionals_df.empty:
        st.info("➕ Procesando archivo Aditionals...")
        
        # Crear diccionario para mapeo rápido de Aditionals
        aditionals_dict = {}
        for idx, row in aditionals_df.iterrows():
            order_id = clean_id(row.get('Order Id', ''))
            if order_id:
                aditionals_dict[order_id] = row
        
        st.info(f"📋 Aditionals indexado: {len(aditionals_dict)} registros")
        
        # Agregar columnas de Aditionals
        aditionals_columns = ['Order Id', 'Item', 'Reference', 'Description', 'Quantity', 'UnitPrice', 'Total']
        
        for col in aditionals_columns:
            if col in aditionals_df.columns:
                consolidated_df[f'aditionals_{col.lower().replace(" ", "_")}'] = np.nan
        
        matched_aditionals = 0
        
        # Hacer matching por prealert_id -> Order Id
        for idx, row in consolidated_df.iterrows():
            prealert_id = clean_id(row.get('prealert_id', ''))
            
            if prealert_id and prealert_id in aditionals_dict:
                aditionals_row = aditionals_dict[prealert_id]
                matched_aditionals += 1
                
                for col in aditionals_columns:
                    if col in aditionals_df.columns:
                        consolidated_df.loc[idx, f'aditionals_{col.lower().replace(" ", "_")}'] = aditionals_row.get(col)
                
                # Debug: mostrar algunos matches
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
        
        # Mostrar las columnas del archivo CXP para debugging
        st.write(f"🔍 Columnas encontradas en CXP: {list(cxp_df.columns)}")
        
        # Normalizar nombres de columnas del archivo CXP (soportar ambos formatos)
        column_mapping = {
            # Formato archivo pequeño -> formato estándar
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
            
            # Formato archivo grande -> formato estándar
            'ot_number': 'OT Number',
            'date': 'Date',
            'consignee': 'Consignee', 
            'co_aereo': 'CO Aereo',
            'arancel': 'Arancel',
            'iva': 'IVA',
            'dest_delivery': 'Dest. Delivery'
        }
        
        # Aplicar mapeo de columnas
        cxp_df_normalized = cxp_df.rename(columns=column_mapping)
        
        # Crear diccionario para mapeo rápido de CXP
        cxp_dict = {}
        for idx, row in cxp_df_normalized.iterrows():
            ref_number = clean_id(row.get('Ref #', ''))
            if ref_number:
                cxp_dict[ref_number] = row
        
        st.info(f"📋 CXP indexado: {len(cxp_dict)} registros")
        
        # Mostrar algunos ejemplos de Ref # para debug
        cxp_refs = list(cxp_dict.keys())[:5]
        st.write(f"🔍 Ejemplos de Ref # en CXP: {cxp_refs}")
        
        # Agregar columnas de CXP (usar todas las columnas disponibles)
        available_cxp_columns = []
        standard_cxp_columns = ['OT Number', 'Date', 'Ref #', 'Consignee', 'CO Aereo', 
                               'Arancel', 'IVA', 'Handling', 'Dest. Delivery', 'Amt. Due', 'Goods Value']
        
        for col in standard_cxp_columns:
            if col in cxp_df_normalized.columns:
                available_cxp_columns.append(col)
                consolidated_df[f'cxp_{col.lower().replace(" ", "_").replace(".", "").replace("#", "number")}'] = np.nan
        
        st.write(f"📊 Columnas CXP que se procesarán: {available_cxp_columns}")
        
        matched_cxp = 0
        
        # Hacer matching por Asignacion -> Ref #
        if 'Asignacion' in consolidated_df.columns:
            # Mostrar algunos ejemplos de Asignacion para debug
            asignaciones = consolidated_df['Asignacion'].dropna().head(5).tolist()
            st.write(f"🔍 Ejemplos de Asignacion calculadas: {asignaciones}")
            
            for idx, row in consolidated_df.iterrows():
                asignacion = clean_id(row.get('Asignacion', ''))
                
                if asignacion and asignacion in cxp_dict:
                    cxp_row = cxp_dict[asignacion]
                    matched_cxp += 1
                    
                    for col in available_cxp_columns:
                        col_name = f'cxp_{col.lower().replace(" ", "_").replace(".", "").replace("#", "number")}'
                        consolidated_df.loc[idx, col_name] = cxp_row.get(col)
                    
                    # Debug: mostrar algunos matches
                    if matched_cxp <= 5:
                        st.write(f"✅ CXP Match {matched_cxp}: Asignacion '{asignacion}' encontrada")
        
        st.success(f"✅ CXP procesado: {matched_cxp} matches por Asignacion")
    
    st.success(f"🎉 Consolidación completada: {len(consolidated_df)} registros finales")
    return consolidated_df

# Función para insertar datos en Supabase
def insert_to_supabase(df):
    """Inserta los datos consolidados en Supabase con validación de duplicados"""
    try:
        st.info("🔍 Preparando datos para inserción...")
        
        # Mapear nombres de columnas del CSV a la base de datos
        df_mapped = map_column_names(df)
        
        # Filtrar solo las columnas que existen en la tabla de la base de datos
        # Estas son las columnas que definimos en la tabla SQL
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
        
        # Agregar columnas de logistics, aditionals y cxp que existan
        for col in df_mapped.columns:
            if (col.startswith('logistics_') or col.startswith('aditionals_') or col.startswith('cxp_')) and col not in db_columns:
                db_columns.append(col)
        
        # Filtrar DataFrame para incluir solo columnas que existen en la DB
        df_filtered = df_mapped[[col for col in db_columns if col in df_mapped.columns]]
        
        st.info(f"📊 Preparando {len(df_filtered)} registros con {len(df_filtered.columns)} columnas")
        
        # Preparar datos para inserción
        records = df_filtered.to_dict('records')
        
        # Limpiar valores NaN y convertir tipos de datos
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
                elif isinstance(value, (np.integer, np.floating)):
                    if np.isfinite(value):
                        record[key] = float(value) if isinstance(value, np.floating) else int(value)
                    else:
                        record[key] = None
        
        # Verificar duplicados por order_id
        order_ids = [r.get('order_id') for r in records if r.get('order_id')]
        if len(set(order_ids)) != len(order_ids):
            st.warning(f"⚠️ Detectados duplicados en order_id. Removiendo duplicados...")
            seen_order_ids = set()
            unique_records = []
            for record in records:
                order_id = record.get('order_id')
                if order_id not in seen_order_ids:
                    seen_order_ids.add(order_id)
                    unique_records.append(record)
            records = unique_records
            st.info(f"✅ Registros únicos: {len(records)}")
        
        # Insertar en lotes
        batch_size = 50
        total_inserted = 0
        errors = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            try:
                result = supabase.table('consolidated_orders').insert(batch).execute()
                total_inserted += len(batch)
                
                progress = min(1.0, (i + batch_size) / len(records))
                progress_bar.progress(progress)
                status_text.text(f"Insertando: {total_inserted}/{len(records)} registros")
                
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
        st.error(f"Error general: {str(e)}")
        return 0

# Interfaz principal
def main():
    st.title("📦 Consolidador de Órdenes")
    st.markdown("### Procesa y consolida archivos según reglas específicas de negocio")
    
    # Sidebar con información
    with st.sidebar:
        st.header("⚙️ Configuración")
        
        st.info("💾 Los datos se guardarán automáticamente en la base de datos")
        
        st.markdown("---")
        st.markdown("**📋 Orden de procesamiento:**")
        st.markdown("1. 📋 **Drapify** (base - obligatorio)")
        st.markdown("2. 🚚 **Logistics** (opcional)")
        st.markdown("   - Match: order_id → Reference")
        st.markdown("   - Fallback: prealert_id → Order number")
        st.markdown("3. ➕ **Aditionals** (opcional)")
        st.markdown("   - Match: prealert_id → Order Id")
        st.markdown("4. 🏷️ **Calcular Asignacion**")
        st.markdown("5. 💰 **CXP** (opcional)")
        st.markdown("   - Match: Asignacion → Ref #")
        st.markdown("6. 💾 **Guardar en Base de Datos**")
    
    # Área principal
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
    
    # Botón de procesamiento
    if st.button("🚀 Procesar y Guardar en BD", disabled=not drapify_file, type="primary"):
        
        with st.spinner("Procesando archivos y guardando en base de datos..."):
            try:
                # Leer archivo Drapify
                if drapify_file.name.endswith('.csv'):
                    drapify_df = pd.read_csv(drapify_file)
                else:
                    drapify_df = pd.read_excel(drapify_file)
                
                st.success(f"✅ Drapify cargado: {len(drapify_df)} registros")
                
                # Mostrar columnas encontradas en Drapify
                with st.expander("🔍 Columnas encontradas en Drapify"):
                    st.write(list(drapify_df.columns))
                
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
                
                # Procesar consolidación usando las reglas específicas
                consolidated_df = process_files_according_to_rules(
                    drapify_df, logistics_df, aditionals_df, cxp_df
                )
                
                # Mostrar preview de los datos
                st.header("👀 Preview de Datos Consolidados")
                st.dataframe(consolidated_df.head(10), use_container_width=True)
                
                # Mostrar estadísticas detalladas
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
                
                # Mostrar información de la columna Asignacion
                if 'Asignacion' in consolidated_df.columns:
                    st.subheader("🏷️ Análisis de Asignaciones")
                    asignacion_counts = consolidated_df['Asignacion'].value_counts().head(10)
                    st.bar_chart(asignacion_counts)
                
                # Guardar automáticamente en base de datos
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
                        st.warning("Los datos fueron procesados correctamente pero no se pudieron guardar")
                
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
                    type="secondary"
                )
                
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
                        st.info("Datos encontrados pero sin columna account_name")
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
