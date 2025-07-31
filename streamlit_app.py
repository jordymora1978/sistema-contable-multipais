import streamlit as st
import pandas as pd
import numpy as np
from supabase import create_client, Client
import os
from datetime import datetime
import io
import time

# NUEVOS IMPORTS PARA UTILIDADES
from modulo_utilidades import get_calculador_utilidades
import plotly.express as px
import plotly.graph_objects as go

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
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_ANON_KEY"]
    return create_client(url, key)

supabase = init_supabase()

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

# Función para corregir acentos y caracteres especiales
def fix_accents(text):
    """Corrige automáticamente todos los caracteres con encoding incorrecto UTF-8"""
    if pd.isna(text) or not isinstance(text, str):
        return text
    
    try:
        # Método principal: decodificar como latin-1 y recodificar como UTF-8
        # Esto soluciona automáticamente la mayoría de problemas de encoding
        result = text.encode('latin-1').decode('utf-8')
        return result
    except (UnicodeDecodeError, UnicodeEncodeError):
        # Si falla el método automático, usar algunos reemplazos básicos muy comunes
        replacements = {
            'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú',
            'Ã±': 'ñ', 'Ã': 'Á', 'Ã‰': 'É', 'Ã"': 'Ó', 'Ãš': 'Ú'
        }
        
        result = str(text)
        for wrong, correct in replacements.items():
            result = result.replace(wrong, correct)
        
        return result

# Función para formatear fechas
def format_date_to_standard(date_value, input_format='auto'):
    """Convierte fechas a formato YYYY-MM-DD"""
    if pd.isna(date_value):
        return None
    
    date_str = str(date_value).strip()
    
    if not date_str or date_str == 'nan':
        return None
    
    try:
        # Si es un número (formato Excel), convertir primero
        if date_str.replace('.', '').isdigit():
            # Es un número de serie de Excel
            excel_date = float(date_str)
            # Convertir desde 1900-01-01 (Excel epoch)
            from datetime import datetime, timedelta
            excel_epoch = datetime(1900, 1, 1)
            actual_date = excel_epoch + timedelta(days=excel_date - 2)  # -2 por bug histórico de Excel
            return actual_date.strftime('%Y-%m-%d')
        
        # Formato YYYY-MM-DD HH:MM (date_created)
        if ' ' in date_str and len(date_str) >= 16:
            date_part = date_str.split(' ')[0]
            if len(date_part) == 10 and date_part.count('-') == 2:
                return date_part
        
        # Formato MM/DD/YYYY (Date de CXP)
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                month, day, year = parts
                return f"{year.zfill(4)}-{month.zfill(2)}-{day.zfill(2)}"
        
        # Formato YYYY-MM-DD ya correcto
        if len(date_str) == 10 and date_str.count('-') == 2:
            return date_str
            
        return date_str  # Devolver original si no se puede convertir
        
    except Exception:
        return date_str

# Función para aplicar formato currency sin decimales
def format_currency_no_decimals(value):
    """Formatea números como currency sin decimales: $#,##0"""
    if pd.isna(value):
        return None
    try:
        num_value = float(value)
        return f"${num_value:,.0f}"
    except (ValueError, TypeError):
        return value

# Función para aplicar formato currency con decimales
def format_currency_with_decimals(value):
    """Formatea números como currency con decimales: $#,##0.00"""
    if pd.isna(value):
        return None
    try:
        num_value = float(value)
        return f"${num_value:,.2f}"
    except (ValueError, TypeError):
        return value

# Función para eliminar duplicados por order_id
def remove_duplicates_by_order_id(df):
    """Elimina filas duplicadas basándose en order_id, manteniendo la primera ocurrencia"""
    if 'order_id' not in df.columns:
        return df
    
    initial_count = len(df)
    
    # Eliminar duplicados basándose en order_id, manteniendo el primero
    df_cleaned = df.drop_duplicates(subset=['order_id'], keep='first')
    
    duplicates_removed = initial_count - len(df_cleaned)
    
    if duplicates_removed > 0:
        st.warning(f"⚠️ Se eliminaron {duplicates_removed} filas duplicadas basándose en order_id")
    else:
        st.success(f"✅ No se encontraron duplicados por order_id")
    
    return df_cleaned

# Función para aplicar todos los formateos
def apply_formatting(df):
    """Aplica todos los formateos especificados al DataFrame"""
    
    st.info("🎨 Aplicando formateos...")
    
    # A) Formato Currency sin decimales: $#,##0
    currency_no_decimals_columns = [
        'unit_price', 'Meli Fee', 'IVA', 'ICA', 'FUENTE', 
        'senders_cost', 'gross_amount', 'net_received_amount', 'net_real_amount'
    ]
    
    for col in currency_no_decimals_columns:
        if col in df.columns:
            df[col] = df[col].apply(format_currency_no_decimals)
            st.write(f"✅ Formato currency sin decimales aplicado a: {col}")
    
    # B) Formato currency con decimales: $#,##0.00
    currency_with_decimals_columns = [
        'profit_price', 'Declare Value', 'data_base_price',
        'logistics_fob', 'logistics_weight', 'logistics_length', 'logistics_width',
        'logistics_height', 'logistics_insurance', 'logistics_logistics',
        'logistics_duties_prealert', 'logistics_duties_pay', 'logistics_duty_fee',
        'logistics_saving', 'logistics_total',
        # AGREGAR COLUMNAS DE CXP CON SUS NOMBRES REALES DESPUÉS DEL MAPEO
        'cxp_co_aereo', 'cxp_arancel', 'cxp_iva', 'cxp_handling', 
        'cxp_dest_delivery', 'cxp_amt_due', 'cxp_goods_value'
    ]
    
    for col in currency_with_decimals_columns:
        if col in df.columns:
            df[col] = df[col].apply(format_currency_with_decimals)
            st.write(f"✅ Formato currency con decimales aplicado a: {col}")
    
    # C) Corregir acentos en todas las columnas de texto
    text_columns = df.select_dtypes(include=['object']).columns
    formatted_columns = currency_no_decimals_columns + currency_with_decimals_columns
    
    for col in text_columns:
        if col not in formatted_columns:
            df[col] = df[col].apply(fix_accents)
    
    st.write(f"✅ Acentos corregidos en {len(text_columns)} columnas de texto")
    
    # D) Formatear fechas
    date_columns = {
        'date_created': 'YYYY-MM-DD HH:MM',
        'cxp_date': 'MM/DD/YYYY o número Excel'
    }
    
    for col, format_desc in date_columns.items():
        if col in df.columns:
            df[col] = df[col].apply(lambda x: format_date_to_standard(x))
            st.write(f"✅ Formato de fecha aplicado a {col} (era: {format_desc})")
    
    # Verificar que fecha_logistics no se sobrescriba en formateo
    if 'fecha_logistics' in df.columns:
        # No aplicar formateo adicional a fecha_logistics ya que viene en formato correcto
        logistics_count = df['fecha_logistics'].notna().sum()
        st.write(f"✅ Fecha logistics preservada en {logistics_count} registros")
    
    # E) Eliminar duplicados por order_id
    df = remove_duplicates_by_order_id(df)
    
    st.success("🎨 Todos los formateos aplicados correctamente")
    
    return df

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

# Función principal para procesar archivos según las reglas especificadas
def process_files_according_to_rules(drapify_df, logistics_df=None, aditionals_df=None, cxp_df=None, logistics_date=None):
    """
    Procesa y consolida todos los archivos según las reglas exactas especificadas:
    1. Drapify como base
    2. Logistics: buscar order_id en Reference, luego prealert_id en Order number
    3. Aditionals: buscar prealert_id en Order Id
    4. Calcular Asignacion
    5. CXP: buscar Asignacion en Ref #
    
    Args:
        logistics_date: Fecha manual asignada a los datos de Logistics
    """
    
    st.info("🔄 Iniciando consolidación según reglas especificadas...")
    
    # PASO 1: Usar Drapify como base (todas las columnas tal como están)
    consolidated_df = drapify_df.copy()
    st.success(f"✅ Archivo base Drapify procesado: {len(consolidated_df)} registros")
    
    # PASO 2: Procesar archivo Logistics si está disponible
    if logistics_df is not None and not logistics_df.empty:
        st.info("🚚 Procesando archivo Logistics...")
        
        # Mostrar la fecha asignada
        if logistics_date:
            st.info(f"📅 Fecha asignada a Logistics: **{logistics_date}**")
        
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
            'Address', 'Phone', 'Master Guide', 'Tariff Position', 'External Id', 'Invoice'
        ]
        
        # Inicializar columnas de Logistics con NaN
        for col in logistics_columns:
            if col in logistics_df.columns:
                consolidated_df[f'logistics_{col.lower().replace(" ", "_")}'] = np.nan
        
        # Agregar la columna de fecha de Logistics
        consolidated_df['fecha_logistics'] = None
        
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
                
                # Asignar la fecha de Logistics configurada
                if logistics_date:
                    consolidated_df.loc[idx, 'fecha_logistics'] = logistics_date.strftime('%Y-%m-%d')
                
                # Debug: mostrar algunos matches
                if (matched_by_order_id + matched_by_prealert_id) <= 5:
                    st.write(f"✅ Match {matched_by_order_id + matched_by_prealert_id}: {match_type} - order_id: {order_id}, prealert_id: {prealert_id}")
        
        st.success(f"✅ Logistics procesado: {matched_by_order_id} matches por order_id, {matched_by_prealert_id} matches por prealert_id")
    else:
        # Si no hay archivo Logistics, agregar la columna vacía para consistencia
        consolidated_df['fecha_logistics'] = None
    
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
                # Mapear el nombre de la columna correctamente
                if col == 'Date':
                    consolidated_df['cxp_date'] = np.nan
                else:
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
                        if col == 'Date':
                            # Formatear la fecha de CXP correctamente
                            date_value = cxp_row.get(col)
                            formatted_date = format_date_to_standard(date_value)
                            consolidated_df.loc[idx, 'cxp_date'] = formatted_date
                        else:
                            col_name = f'cxp_{col.lower().replace(" ", "_").replace(".", "").replace("#", "number")}'
                            consolidated_df.loc[idx, col_name] = cxp_row.get(col)
                    
                    # Debug: mostrar algunos matches
                    if matched_cxp <= 5:
                        st.write(f"✅ CXP Match {matched_cxp}: Asignacion '{asignacion}' encontrada, fecha: {formatted_date if 'Date' in available_cxp_columns else 'N/A'}")
        
        st.success(f"✅ CXP procesado: {matched_cxp} matches por Asignacion")
    
    st.success(f"🎉 Consolidación completada: {len(consolidated_df)} registros finales")
    return consolidated_df

# Función para insertar datos en Supabase
def insert_to_supabase(df):
    """Inserta los datos consolidados en Supabase"""
    try:
        # Preparar datos para inserción
        records = df.to_dict('records')
        
        # Limpiar valores NaN
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
        
        # Insertar en lotes
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

# ===============================================
# FUNCIÓN PRINCIPAL CON ROUTING
# ===============================================

def main():
    st.title("💰 Sistema de Gestión Integral")
    st.markdown("### Consolidación de archivos y cálculo de utilidades")
    
    # Sidebar con navegación expandida
    with st.sidebar:
        st.image("https://via.placeholder.com/150x50/4F46E5/white?text=LOGO", width=150)
        st.markdown("---")
        
        # NAVEGACIÓN EXPANDIDA
        pagina = st.selectbox(
            "📋 Navegación",
            [
                "🏠 Consolidador de Archivos",
                "💰 Cálculo de Utilidades",
                "💱 Gestión TRM",
                "📊 Dashboard Utilidades",
                "📋 Reportes"
            ]
        )
        
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
    
    # ROUTING DE PÁGINAS
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
# PÁGINA DEL CONSOLIDADOR (TU CÓDIGO ORIGINAL)
# ===============================================

def mostrar_consolidador(processing_mode):
    """Página del consolidador de archivos"""
    
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
        
        # Fecha manual para Logistics
        logistics_date = None
        if logistics_file:
            st.markdown("**📅 Fecha de Datos de Logistics:**")
            col_date1, col_date2 = st.columns([2, 1])
            
            with col_date1:
                logistics_date = st.date_input(
                    "Fecha de estos datos de Logistics",
                    value=datetime.now().date(),
                    help="Esta fecha se usará para reportes y análisis de costos"
                )
            
            with col_date2:
                if st.button("📅 Usar Hoy", key="use_today"):
                    logistics_date = datetime.now().date()
                    st.rerun()
            
            st.info(f"💡 Los datos de Logistics se marcarán con fecha: **{logistics_date}**")
        
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
    if st.button("🚀 Procesar Archivos", disabled=not drapify_file, type="primary"):
        
        with st.spinner("Procesando archivos según reglas de negocio..."):
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
                    drapify_df, logistics_df, aditionals_df, cxp_df, logistics_date
                )
                
                # APLICAR FORMATEOS DESPUÉS DE LA CONSOLIDACIÓN
                st.header("🎨 Aplicando Formateos")
                consolidated_df = apply_formatting(consolidated_df)
                
                # Mostrar preview de los datos
                st.header("👀 Preview de Datos Consolidados")
                st.dataframe(consolidated_df.head(10), use_container_width=True)
                
                # Mostrar información adicional sobre formateos
                st.subheader("📊 Información de Procesamiento")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**💰 Columnas con formato Currency sin decimales:**")
                    currency_no_dec = ['unit_price', 'Meli Fee', 'IVA', 'ICA', 'FUENTE', 
                                      'senders_cost', 'gross_amount', 'net_received_amount', 'net_real_amount']
                    for col in currency_no_dec:
                        if col in consolidated_df.columns:
                            st.write(f"• {col}")
                
                with col2:
                    st.markdown("**💎 Columnas con formato Currency con decimales:**")
                    currency_with_dec = ['profit_price', 'Declare Value', 'data_base_price',
                                        'logistics_fob', 'logistics_weight', 'logistics_total']
                    for col in currency_with_dec:
                        if col in consolidated_df.columns:
                            st.write(f"• {col}")
                
                # Mostrar información de fechas
                if 'fecha_logistics' in consolidated_df.columns:
                    logistics_dates = consolidated_df['fecha_logistics'].dropna().unique()
                    if len(logistics_dates) > 0:
                        st.write(f"📅 **Fecha de Logistics aplicada:** {logistics_dates[0]}")
                        registros_con_fecha = consolidated_df['fecha_logistics'].notna().sum()
                        st.write(f"📊 **Registros con fecha Logistics:** {registros_con_fecha}")
                
                # Mostrar muestras de fechas formateadas
                if 'date_created' in consolidated_df.columns:
                    sample_dates = consolidated_df['date_created'].dropna().head(3).tolist()
                    st.write(f"📅 **Ejemplos de fechas Drapify:** {sample_dates}")
                
                if 'cxp_date' in consolidated_df.columns:
                    sample_cxp_dates = consolidated_df['cxp_date'].dropna().head(3).tolist()
                    st.write(f"📅 **Ejemplos de fechas CXP:** {sample_cxp_dates}")
                
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
                
                # Insertar en base de datos si se seleccionó
                if processing_mode == "Consolidar e insertar en DB":
                    st.header("💾 Insertar en Base de Datos")
                    
                    if st.button("🚀 Insertar en Supabase", type="secondary"):
                        with st.spinner("Insertando datos en Supabase..."):
                            inserted_count = insert_to_supabase(consolidated_df)
                            
                            if inserted_count > 0:
                                st.success(f"✅ {inserted_count} registros insertados exitosamente!")
                                
                                # Log del procesamiento
                                try:
                                    log_data = {
                                        'file_type': 'consolidated',
                                        'records_processed': len(consolidated_df),
                                        'records_matched': inserted_count,
                                        'status': 'success'
                                    }
                                    supabase.table('processing_logs').insert(log_data).execute()
                                except:
                                    pass
                            else:
                                st.error("❌ Error insertando datos")
                
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
                result = supabase.table('orders').select('account_name').execute()
                
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
                result = supabase.table('orders').select('*').order('created_at', desc=True).limit(10).execute()
                
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
            query = supabase.table('orders').select('*')
            
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

# ===============================================
# PÁGINAS DE UTILIDADES
# ===============================================

def mostrar_calculo_utilidades():
    """Página principal de cálculo de utilidades"""
    st.title("💰 Cálculo de Utilidades")
    st.markdown("### Procesamiento automático según reglas de negocio")
    
    # Obtener calculador
    try:
        calculador = get_calculador_utilidades()
    except Exception as e:
        st.error(f"❌ Error inicializando calculador: {str(e)}")
        return
    
    # Tabs para organizar funcionalidad
    tab1, tab2, tab3 = st.tabs(["🔄 Calcular", "📊 Resultados", "⚙️ Configuración"])
    
    with tab1:
        st.subheader("🔄 Calcular Utilidades desde Órdenes Consolidadas")
        
        # Opciones de filtrado
        col1, col2, col3 = st.columns(3)
        
        with col1:
            cuenta_filtro = st.selectbox(
                "Filtrar por cuenta:",
                ["Todas", "1-TODOENCARGO-CO", "2-MEGATIENDA SPA", "3-VEENDELO", 
                 "4-MEGA TIENDAS PERUANAS", "5-DETODOPARATODOS", "6-COMPRAFACIL", 
                 "7-COMPRA-YA", "8-FABORCARGO"]
            )
        
        with col2:
            limite_registros = st.number_input(
                "Límite de registros:", 
                min_value=10, 
                max_value=10000, 
                value=100,
                step=50
            )
        
        with col3:
            solo_sin_utilidades = st.checkbox(
                "Solo órdenes nuevas",
                value=True,
                help="Procesar solo órdenes que no tienen utilidades calculadas"
            )
        
        if st.button("🚀 Calcular Utilidades", type="primary", use_container_width=True):
            with st.spinner("🔄 Obteniendo órdenes consolidadas..."):
                try:
                    # Construir query
                    query = supabase.table('orders').select('*').limit(limite_registros)
                    
                    if cuenta_filtro != "Todas":
                        query = query.eq('account_name', cuenta_filtro)
                    
                    # Obtener datos
                    result = query.execute()
                    
                    if result.data:
                        df_ordenes = pd.DataFrame(result.data)
                        st.success(f"✅ {len(df_ordenes)} órdenes obtenidas")
                        
                        # Mostrar preview
                        with st.expander("👀 Preview de datos"):
                            st.dataframe(df_ordenes.head(), use_container_width=True)
                        
                        # Verificar columnas necesarias
                        columnas_necesarias = ['Serial#', 'order_id', 'account_name', 'Declare Value', 'quantity', 'net_real_amount']
                        columnas_faltantes = [col for col in columnas_necesarias if col not in df_ordenes.columns]
                        
                        if columnas_faltantes:
                            st.error(f"❌ Faltan columnas necesarias: {columnas_faltantes}")
                            return
                        
                        # Calcular utilidades
                        st.info("🔄 Calculando utilidades...")
                        df_utilidades = calculador.calcular_utilidades_por_cuenta(df_ordenes)
                        
                        # Mostrar resultados
                        st.success("✅ Utilidades calculadas exitosamente!")
                        
                        # Métricas
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            total_utilidad = df_utilidades['Utilidad Gss'].sum()
                            st.metric("💰 Utilidad Total", f"${total_utilidad:,.2f}")
                        
                        with col2:
                            ordenes_positivas = (df_utilidades['Utilidad Gss'] > 0).sum()
                            st.metric("📈 Órdenes Positivas", ordenes_positivas)
                        
                        with col3:
                            ordenes_negativas = (df_utilidades['Utilidad Gss'] < 0).sum()
                            st.metric("📉 Órdenes Negativas", ordenes_negativas)
                        
                        with col4:
                            utilidad_promedio = df_utilidades['Utilidad Gss'].mean()
                            st.metric("📊 Utilidad Promedio", f"${utilidad_promedio:.2f}")
                        
                        # Tabla de resultados
                        st.subheader("📋 Resultados Detallados")
                        st.dataframe(df_utilidades, use_container_width=True)
                        
                        # Gráfico por cuenta
                        if len(df_utilidades['account_name'].unique()) > 1:
                            st.subheader("📊 Utilidades por Cuenta")
                            utilidad_por_cuenta = df_utilidades.groupby('account_name')['Utilidad Gss'].sum().reset_index()
                            fig = px.bar(utilidad_por_cuenta, x='account_name', y='Utilidad Gss',
                                       title="Utilidad Total por Cuenta")
                            st.plotly_chart(fig, use_container_width=True)
                        
                        # Opciones de guardado y descarga
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if st.button("💾 Guardar en Base de Datos", use_container_width=True):
                                if calculador.guardar_utilidades_en_bd(df_utilidades):
                                    st.success("✅ Utilidades guardadas en base de datos!")
                                    st.rerun()
                        
                        with col2:
                            csv = df_utilidades.to_csv(index=False)
                            st.download_button(
                                label="📥 Descargar CSV",
                                data=csv,
                                file_name=f"utilidades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                    
                    else:
                        st.warning("⚠️ No se encontraron órdenes con los filtros especificados")
                
                except Exception as e:
                    st.error(f"❌ Error obteniendo órdenes: {str(e)}")
                    st.exception(e)
    
    with tab2:
        st.subheader("📊 Resultados Guardados")
        
        try:
            # Obtener estadísticas desde la vista
            result = supabase.table('estadisticas_utilidades').select('*').execute()
            
            if result.data:
                stats_df = pd.DataFrame(result.data)
                st.dataframe(stats_df, use_container_width=True)
                
                # Gráfico de estadísticas
                fig = px.bar(stats_df, x='account_name', y='utilidad_total_gss',
                           title="Utilidad Total por Cuenta (Guardadas)")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("📝 No hay utilidades calculadas guardadas aún")
        
        except Exception as e:
            st.error(f"❌ Error cargando estadísticas: {str(e)}")
    
    with tab3:
        st.subheader("⚙️ Configuración del Sistema")
        
        # Mostrar TRM actual
        st.markdown("**💱 TRM Actual:**")
        for pais, valor in calculador.trm_actual.items():
            st.write(f"🇺🇸 {pais.title()}: ${valor:,.4f}")
        
        st.info("💡 Para cambiar TRM, usar la página 'Gestión TRM'")

def mostrar_gestion_trm():
    """Página de gestión de TRM"""
    st.title("💱 Gestión de TRM")
    st.markdown("### Control de Tasas Representativas del Mercado")
    
    try:
        calculador = get_calculador_utilidades()
    except Exception as e:
        st.error(f"❌ Error inicializando calculador: {str(e)}")
        return
    
    # Configuración TRM
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("⚙️ Configurar TRM")
        
        col_co, col_pe, col_cl = st.columns(3)
        
        with col_co:
            trm_colombia = st.number_input(
                "🇨🇴 TRM Colombia (COP/USD)", 
                value=calculador.trm_actual.get('colombia', 4250.0), 
                step=0.01,
                format="%.2f"
            )
        
        with col_pe:
            trm_peru = st.number_input(
                "🇵🇪 TRM Perú (PEN/USD)", 
                value=calculador.trm_actual.get('peru', 3.75), 
                step=0.01,
                format="%.2f"
            )
        
        with col_cl:
            trm_chile = st.number_input(
                "🇨🇱 TRM Chile (CLP/USD)", 
                value=calculador.trm_actual.get('chile', 850.0), 
                step=0.01,
                format="%.2f"
            )
        
        if st.button("💾 Actualizar TRM", type="primary"):
            nuevas_trm = {
                'colombia': trm_colombia,
                'peru': trm_peru,
                'chile': trm_chile
            }
            
            if calculador.actualizar_trm(nuevas_trm, "usuario_streamlit"):
                st.success("✅ TRM actualizada exitosamente!")
                st.rerun()
    
    with col2:
        st.subheader("📊 Estado TRM")
        
        for pais, valor in calculador.trm_actual.items():
            st.metric(f"🇺🇸 {pais.title()}", f"${valor:,.4f}")
        
        st.info("🔄 Última actualización: " + datetime.now().strftime("%H:%M:%S"))
    
    # Historial de cambios
    st.subheader("📋 Historial de Cambios")
    
    try:
        # Obtener historial directamente de Supabase
        result = supabase.table('trm_history').select('*').order('fecha_cambio', desc=True).limit(20).execute()
        
        if result.data:
            historial_df = pd.DataFrame(result.data)
            st.dataframe(historial_df, use_container_width=True)
            
            # Gráfico de evolución
            if len(historial_df) > 1:
                fig = px.line(historial_df, x='fecha_cambio', y='valor_nuevo', 
                            color='pais', title="Evolución TRM")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📝 No hay cambios registrados")
    
    except Exception as e:
        st.error(f"❌ Error cargando historial: {str(e)}")

def mostrar_dashboard_utilidades():
    """Dashboard de utilidades"""
    st.title("📊 Dashboard de Utilidades")
    st.markdown("### Panel de control y métricas")
    
    try:
        # Obtener estadísticas desde la vista
        result = supabase.table('estadisticas_utilidades').select('*').execute()
        
        if result.data:
            stats_df = pd.DataFrame(result.data)
            
            # KPIs principales
            col1, col2, col3, col4 = st.columns(4)
            
            total_utilidad = stats_df['utilidad_total_gss'].sum()
            total_ordenes = stats_df['total_ordenes'].sum()
            ordenes_positivas = stats_df['ordenes_positivas'].sum()
            ordenes_negativas = stats_df['ordenes_negativas'].sum()
            
            with col1:
                st.metric("💰 Utilidad Total", f"${total_utilidad:,.2f}")
            
            with col2:
                st.metric("📦 Total Órdenes", f"{total_ordenes:,}")
            
            with col3:
                st.metric("📈 Órdenes Positivas", f"{ordenes_positivas:,}")
            
            with col4:
                st.metric("📉 Órdenes Negativas", f"{ordenes_negativas:,}")
            
            # Gráficos
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📊 Utilidad por Cuenta")
                fig = px.bar(stats_df, x='account_name', y='utilidad_total_gss',
                           title="Utilidad Total por Cuenta")
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("📈 Performance por Cuenta")
                fig = px.scatter(stats_df, x='total_ordenes', y='utilidad_promedio_gss',
                               color='account_name', size='utilidad_total_gss',
                               title="Órdenes vs Utilidad Promedio")
                st.plotly_chart(fig, use_container_width=True)
            
            # Tabla detallada
            st.subheader("📋 Detalle por Cuenta")
            st.dataframe(stats_df, use_container_width=True)
        
        else:
            st.info("📝 No hay datos de utilidades para mostrar")
            st.markdown("💡 Primero calcula utilidades en la página 'Cálculo de Utilidades'")
    
    except Exception as e:
        st.error(f"❌ Error cargando dashboard: {str(e)}")

def mostrar_reportes():
    """Página de reportes"""
    st.title("📋 Reportes")
    st.markdown("### Generación de reportes automáticos")
    
    st.info("🚧 Funcionalidad en desarrollo")
    
    # Preview de funcionalidad futura
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
# EJECUTAR LA APLICACIÓN
# ===============================================

if __name__ == "__main__":
    main()
