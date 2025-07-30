import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import os
from datetime import datetime
import numpy as np

# --- Configuración de Streamlit ---
st.set_page_config(
    page_title="Sistema Contable Multi-País - Carga de Datos",
    page_icon="📁",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Configuración de Supabase ---
@st.cache_resource
def init_supabase_client():
    """Inicializa la conexión con Supabase, usando st.secrets o valores de fallback."""
    try:
        url = st.secrets.get("supabase", {}).get("url", "https://qzexuqkedukcwcyhrpza.supabase.co")
        key = st.secrets.get("supabase", {}).get("key", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0")
        
        if not url or not key:
            st.error("Error: Las credenciales de Supabase no están configuradas correctamente.")
            st.stop()

        return create_client(url, key)
    except Exception as e:
        st.error(f"Error crítico al inicializar Supabase: {e}")
        st.exception(e)
        st.stop()

supabase: Client = init_supabase_client()

# --- Función para obtener columnas válidas de Supabase ---
@st.cache_data(ttl=300)
def get_valid_supabase_columns():
    """Obtiene las columnas válidas de la tabla 'orders'."""
    try:
        # Intentar SELECT directo
        result = supabase.table('orders').select('*').limit(1).execute()
        if result.data and len(result.data) > 0:
            return list(result.data[0].keys())
        
        # Si la tabla está vacía, retornar las columnas que sabemos que existen
        # Esto es un fallback importante para esquemas vacíos o recién creados.
        return [
            'id', 'created_at', 'order_id_drapify', 'system_hash', 'serial_hash',
            'account_name', 'date_created', 'quantity_drapify', 'logistic_type',
            'order_status_meli', 'etiqueta_envio', 'declare_value', 'net_real_amount',
            'logistic_weight_lbs', 'refunded_date', 'order_number_anican', 
            'reference_anican', 'fob', 'insurance', 'logistics_anican',
            'duties_prealert', 'duties_pay', 'duty_fee', 'saving', 'total_anican',
            'external_id', 'date_cxp', 'ref_hash_cxp', 'co_aereo', 'arancel_cxp',
            'iva_cxp', 'handling', 'dest_delivery', 'amt_due_cxp', 'goods_value',
            'order_id_aditionals', 'quantity_aditionals', 'unit_price_aditionals',
            'asignacion', 'processed_at_app'
        ]
        
    except Exception as e:
        st.error(f"Error al obtener columnas de Supabase: {e}")
        return []

# --- Mapeo de columnas CORREGIDO ---
# Este mapeo define cómo las columnas de tus DataFrames (después de la limpieza)
# se corresponden con los nombres de las columnas en tu base de datos Supabase.
supabase_db_schema_mapping = {
    # Columnas de DRAPIFY (nombres después del procesamiento → nombres en DB)
    'system_hash': 'system_hash',
    'serial_hash': 'serial_hash',
    'order_id_drapify': 'order_id_drapify',
    'account_name': 'account_name',
    'date_created': 'date_created',
    'quantity_drapify': 'quantity_drapify',
    'logistic_type': 'logistic_type',
    'order_status_meli': 'order_status_meli',
    'etiqueta_envio': 'etiqueta_envio',
    'declare_value_drapify': 'declare_value', 
    'net_real_amount': 'net_real_amount',
    'logistic_weight_lbs': 'logistic_weight_lbs',
    'refunded_date': 'refunded_date',

    # Columnas de ANICAN LOGISTICS (nombres después del procesamiento → nombres en DB)
    'order_number_anican': 'order_number_anican',
    'reference_anican': 'reference_anican',
    'fob': 'fob',
    'insurance': 'insurance',
    'logistics_anican_value': 'logistics_anican', 
    'duties_prealert': 'duties_prealert',
    'duties_pay': 'duties_pay',
    'duty_fee': 'duty_fee',
    'saving': 'saving',
    'total_anican_value': 'total_anican', 
    'external_id_anican': 'external_id', 

    # Columnas de CXP (nombres después del procesamiento → nombres en DB)
    'date': 'date_cxp', 
    'ref_hash': 'ref_hash_cxp', 
    'co_aereo': 'co_aereo',
    'arancel': 'arancel_cxp', 
    'iva': 'iva_cxp', 
    'handling': 'handling',
    'dest_delivery': 'dest_delivery',
    'amt_due': 'amt_due_cxp', # Esta fue la columna clave que corregimos en el mapeo
    'goods_value': 'goods_value',

    # Columnas de ADITIONALS (nombres después del procesamiento → nombres en DB)
    'order_id_aditionals': 'order_id_aditionals',
    'quantity_aditionals': 'quantity_aditionals',
    'unit_price_aditionals': 'unit_price_aditionals',

    # Columna calculada
    'asignacion': 'asignacion',
    
    # Metadatos
    'processed_at_app': 'processed_at_app'
}

# --- Funciones de Utilidad ---
def leer_excel_o_csv(uploaded_file):
    """Lee un archivo Excel o CSV y lo devuelve como DataFrame."""
    try:
        uploaded_file.seek(0)
        if uploaded_file.name.endswith('.csv'):
            try:
                # Intentar sniffer para detectar dialecto (separador, etc.)
                sample = uploaded_file.getvalue().decode('utf-8').splitlines()[:5]
                sniffer = pd.io.common.DialectSniffer()
                dialect = sniffer.sniff(''.join(sample))
                uploaded_file.seek(0) # Resetear puntero después de leer sample
                return pd.read_csv(uploaded_file, dialect=dialect, dtype=str)
            except Exception:
                # Fallback si el sniffer falla
                uploaded_file.seek(0)
                return pd.read_csv(uploaded_file, dtype=str)
        elif uploaded_file.name.endswith(('.xlsx', '.xls')):
            return pd.read_excel(uploaded_file, dtype=str)
        else:
            st.error(f"Formato de archivo no soportado: **{uploaded_file.name}**. Por favor, sube un CSV o Excel.")
            return None
    except Exception as e:
        st.error(f"Error al leer el archivo **{uploaded_file.name}**: {e}")
        st.exception(e)
        return None

def limpiar_nombres_columnas_generico(df):
    """Limpia los nombres de las columnas de un DataFrame a snake_case."""
    if df is None:
        return None
    new_columns = []
    for col in df.columns:
        # Convertir a string, quitar espacios iniciales/finales, reemplazar caracteres especiales, y a minúsculas
        cleaned_col = str(col).strip().replace('#', 'hash').replace(' ', '_').replace('.', '').replace('-', '_').lower()
        new_columns.append(cleaned_col)
    df.columns = new_columns
    return df

def try_read_cxp(uploaded_file):
    """Intenta leer el archivo CXP probando diferentes filas como encabezado."""
    if uploaded_file is None:
        return None

    file_content = uploaded_file.getvalue()

    # Intentar con diferentes filas como encabezado (0-4)
    for header_row_index in range(5):
        try:
            file_stream = io.BytesIO(file_content) # Crear un nuevo stream para cada intento
            if uploaded_file.name.endswith('.csv'):
                df_test = pd.read_csv(file_stream, header=header_row_index, dtype=str)
            else:
                df_test = pd.read_excel(file_stream, header=header_row_index, dtype=str)
            
            # Renombrado explícito de columnas clave de CXP (antes de la limpieza genérica)
            cxp_renames = {
                'Ref #': 'ref_hash',
                'Amt. Due': 'amt_due',
                'Date': 'date',
                'CO Aereo': 'co_aereo',
                'Arancel': 'arancel',
                'IVA': 'iva',
                'Handling': 'handling',
                'Dest. Delivery': 'dest_delivery',
                'Goods Value': 'goods_value',
            }
            
            current_cols_copy = df_test.columns.copy()
            # Crear mapa de renombrado solo para las columnas que realmente existen
            rename_map = {orig_name: new_name for orig_name, new_name in cxp_renames.items() if orig_name in current_cols_copy}
            df_test = df_test.rename(columns=rename_map)
            
            df_test_cleaned = limpiar_nombres_columnas_generico(df_test.copy())
            
            # Verificar si las columnas clave existen después de la limpieza y renombrado
            ref_col_found = next((col for col in df_test_cleaned.columns if 'ref_hash' in col), None)
            amt_due_col_found = next((col for col in df_test_cleaned.columns if 'amt_due' in col), None)

            if ref_col_found and amt_due_col_found:
                st.info(f"✨ Encabezado del archivo CXP detectado en la fila: **{header_row_index + 1}**")
                return df_test_cleaned
        except Exception:
            # Si hay un error al leer con ese encabezado, simplemente intentamos el siguiente
            pass

    st.error("❌ No se pudieron encontrar las columnas clave ('Ref #' y 'Amt. Due') en el archivo CXP. Por favor, verifica el formato.")
    return None

def process_files_for_upload(drapify_file, anican_logistics_file, cxp_file, aditionals_file):
    """Procesa y une todos los DataFrames de los archivos cargados."""
    
    st.subheader("Paso 1: Cargando y Limpiando Archivos Individuales")

    # --- Cargar DRAPIFY (archivo base) ---
    df_drapify = pd.DataFrame() # Inicializar para asegurar que siempre exista
    if drapify_file:
        df_drapify_raw = leer_excel_o_csv(drapify_file)
        if df_drapify_raw is not None:
            # Renombrado explícito de columnas clave de DRAPIFY antes de la limpieza genérica
            drapify_renames = {
                'System#': 'System_Hash', 
                'Serial#': 'Serial_Hash', 
                'order_id': 'Order_ID',
                'account_name': 'Account_Name',
                'date_created': 'Date_Created',
                'quantity': 'Quantity_Drapify',
                'logistic_type': 'Logistic_Type',
                'order_status_meli': 'Order_Status_Meli',
                'ETIQUETA_ENVIO': 'Etiqueta_Envio',
                'Declare Value': 'Declare_Value_Drapify',
                'net_real_amount': 'Net_Real_Amount',
                'logistic_weight_lbs': 'Logistic_Weight_Lbs',
                'refunded_date': 'Refunded_Date',
            }
            
            current_cols_copy = df_drapify_raw.columns.copy()
            rename_map = {orig_name: new_name for orig_name, new_name in drapify_renames.items() if orig_name in current_cols_copy}
            df_drapify = df_drapify_raw.rename(columns=rename_map)
            
            df_drapify = limpiar_nombres_columnas_generico(df_drapify)

            # Convertir columnas numéricas
            for col in ['quantity_drapify', 'declare_value_drapify', 'net_real_amount', 'logistic_weight_lbs']:
                if col in df_drapify.columns:
                    df_drapify[col] = pd.to_numeric(df_drapify[col], errors='coerce').fillna(0)
            
            # Asegurar que las columnas de texto sean string
            for col in ['order_id', 'system_hash', 'serial_hash', 'account_name', 'order_status_meli', 'logistic_type', 'etiqueta_envio']:
                if col in df_drapify.columns:
                    df_drapify[col] = df_drapify[col].astype(str).str.strip()

            st.info("✅ Archivo **DRAPIFY** cargado y columnas limpias.")
        else:
            st.error("❌ No se pudo cargar el archivo **DRAPIFY**. Es un archivo requerido.")
            return pd.DataFrame() # Retornar un DataFrame vacío si el archivo base falla
    else:
        st.warning("⚠️ No se ha cargado el archivo **DRAPIFY**. Por favor, cárgalo para continuar.")
        return pd.DataFrame() # Retornar un DataFrame vacío si el archivo base no se sube

    # Iniciar el DataFrame procesado con Drapify, renombrando la clave de unión
    df_processed = df_drapify.rename(columns={'order_id': 'order_id_drapify'}).copy()

    # --- Cargar ANICAN LOGISTICS ---
    df_anican = pd.DataFrame() # Inicializar
    if anican_logistics_file:
        df_anican_raw = leer_excel_o_csv(anican_logistics_file)
        if df_anican_raw is not None:
            anican_renames = {
                'Order number': 'Order_Number_Anican',
                'Reference': 'Reference_Anican',
                'FOB': 'FOB', 
                'Insurance': 'Insurance',
                'Logistics': 'Logistics_Anican_Value',
                'Duties Prealert': 'Duties_Prealert',
                'Duties Pay': 'Duties_Pay',
                'Duty Fee': 'Duty_Fee',
                'Saving': 'Saving',
                'Total': 'Total_Anican_Value',
                'External Id': 'External_ID_Anican',
            }
            
            current_cols_copy = df_anican_raw.columns.copy()
            rename_map = {orig_name: new_name for orig_name, new_name in anican_renames.items() if orig_name in current_cols_copy}
            df_anican = df_anican_raw.rename(columns=rename_map)
            df_anican = limpiar_nombres_columnas_generico(df_anican)

            # Asegurar tipos de datos
            for col in ['order_number_anican', 'reference_anican', 'external_id_anican']:
                if col in df_anican.columns:
                    df_anican[col] = df_anican[col].astype(str).str.strip()
            
            for col in ['fob', 'insurance', 'logistics_anican_value', 'duties_prealert', 'duties_pay', 'duty_fee', 'saving', 'total_anican_value']:
                if col in df_anican.columns:
                    df_anican[col] = pd.to_numeric(df_anican[col], errors='coerce').fillna(0)

            st.info("✅ Archivo **Anican Logistics** cargado y columnas limpias.")
        else:
            st.warning("❌ No se pudo cargar el archivo **Anican Logistics**.")
    else:
        st.info("ℹ️ Archivo **Anican Logistics** no proporcionado. Se omitirá esta unión.")


    # --- Cargar ADITIONALS ---
    df_aditionals = pd.DataFrame() # Inicializar
    if aditionals_file:
        df_aditionals_raw = leer_excel_o_csv(aditionals_file)
        if df_aditionals_raw is not None:
            aditionals_renames = {
                'Order Id': 'Order_ID_Aditionals',
                'Quantity': 'Quantity_Aditionals',
                'UnitPrice': 'Unit_Price_Aditionals',
            }
            
            current_cols_copy = df_aditionals_raw.columns.copy()
            rename_map = {orig_name: new_name for orig_name, new_name in aditionals_renames.items() if orig_name in current_cols_copy}
            df_aditionals = df_aditionals_raw.rename(columns=rename_map)
            df_aditionals = limpiar_nombres_columnas_generico(df_aditionals)

            if 'order_id_aditionals' in df_aditionals.columns:
                df_aditionals['order_id_aditionals'] = df_aditionals['order_id_aditionals'].astype(str).str.strip()
            
            for col in ['quantity_aditionals', 'unit_price_aditionals']:
                if col in df_aditionals.columns:
                    df_aditionals[col] = pd.to_numeric(df_aditionals[col], errors='coerce').fillna(0)
            
            st.info("✅ Archivo **Aditionals** cargado y columnas limpias.")
        else:
            st.warning("❌ No se pudo cargar el archivo **Aditionals**.")
    else:
        st.info("ℹ️ Archivo **Aditionals** no proporcionado. Se omitirá esta unión.")

    # --- Cargar CXP ---
    df_cxp = pd.DataFrame() # Inicializar
    if cxp_file:
        df_cxp = try_read_cxp(cxp_file) # Usar la función inteligente para CXP
        if df_cxp is not None:
            if 'ref_hash' in df_cxp.columns:
                df_cxp['ref_hash'] = df_cxp['ref_hash'].astype(str).str.strip()
            
            for col in ['co_aereo', 'arancel', 'iva', 'handling', 'dest_delivery', 'amt_due', 'goods_value']:
                if col in df_cxp.columns:
                    df_cxp[col] = pd.to_numeric(df_cxp[col], errors='coerce').fillna(0)
            
            st.info("✅ Archivo **CXP** cargado y columnas limpias.")
        else:
            st.warning("❌ No se pudo cargar el archivo **CXP**.")
    else:
        st.info("ℹ️ Archivo **CXP** no proporcionado. Se omitirá esta unión.")

    # --- Uniones de DataFrames ---
    st.subheader("Paso 2: Uniendo DataFrames")

    # 1. Unir Drapify con Anican Logistics
    # La clave de unión para Drapify es 'order_id_drapify'
    # La clave de unión para Anican Logistics es 'reference_anican'
    if not df_anican.empty and 'reference_anican' in df_anican.columns and 'order_id_drapify' in df_processed.columns:
        df_processed = pd.merge(df_processed, df_anican,
                                 left_on='order_id_drapify', right_on='reference_anican',
                                 how='left', suffixes=('', '_anican_logistics'))
        st.success("✅ **Drapify** unido con **Anican Logistics**.")
    elif not df_anican.empty:
        st.warning("⚠️ No se pudo unir **Drapify** con **Anican Logistics**. Verifique las columnas 'order_id_drapify' o 'reference_anican'.")
    else:
        st.info("ℹ️ Omisión de unión con **Anican Logistics** (archivo no cargado o vacío).")

    # 2. Calcular columna 'Asignacion' antes de unir con CXP y Aditionals
    # Se calcula con base en df_processed, después de la primera unión si Anican existe.
    df_processed['asignacion'] = None # Inicializar la columna
    if 'account_name' in df_processed.columns and 'serial_hash' in df_processed.columns:
        df_processed['serial_hash'] = df_processed['serial_hash'].astype(str).str.strip()
        df_processed['account_name'] = df_processed['account_name'].astype(str).str.strip()

        # Aplicar la lógica de asignación
        df_processed.loc[df_processed['account_name'] == "1-TODOENCARGO-CO", 'asignacion'] = "TDC" + df_processed['serial_hash']
        df_processed.loc[df_processed['account_name'] == "2-MEGATIENDA SPA", 'asignacion'] = "MEGA" + df_processed['serial_hash']
        df_processed.loc[df_processed['account_name'] == "4-MEGA TIENDAS PERUANAS", 'asignacion'] = "MGA-PE" + df_processed['serial_hash']
        df_processed.loc[df_processed['account_name'] == "5-DETODOPARATODOS", 'asignacion'] = "DTPT" + df_processed['serial_hash']
        df_processed.loc[df_processed['account_name'] == "6-COMPRAFACIL", 'asignacion'] = "CFA" + df_processed['serial_hash']
        df_processed.loc[df_processed['account_name'] == "7-COMPRA-YA", 'asignacion'] = "CPYA" + df_processed['serial_hash']
        df_processed.loc[df_processed['account_name'] == "8-FABORCARGO", 'asignacion'] = "FBC" + df_processed['serial_hash']

        df_processed['asignacion'] = df_processed['asignacion'].astype(str).str.strip()
        st.success("✅ Columna **'Asignacion'** calculada.")
    else:
        st.warning("⚠️ No se pudo calcular la columna **'Asignacion'**. Faltan 'account_name' o 'serial_hash'.")

    # 3. Unir con Aditionals
    # La clave de unión para df_processed es 'order_number_anican'
    # La clave de unión para Aditionals es 'order_id_aditionals'
    if not df_aditionals.empty and 'order_id_aditionals' in df_aditionals.columns and 'order_number_anican' in df_processed.columns:
        df_processed = pd.merge(df_processed, df_aditionals,
                                 left_on='order_number_anican', right_on='order_id_aditionals',
                                 how='left', suffixes=('', '_aditionals_merged'))
        st.success("✅ Datos unidos con **Aditionals**.")
    elif not df_aditionals.empty:
        st.warning("⚠️ No se pudo unir con **Aditionals**. Verifique las columnas 'order_number_anican' o 'order_id_aditionals'.")
    else:
        st.info("ℹ️ Omisión de unión con **Aditionals** (archivo no cargado o vacío).")

    # 4. Unir con CXP
    # La clave de unión para df_processed es 'asignacion'
    # La clave de unión para CXP es 'ref_hash'
    if not df_cxp.empty and 'ref_hash' in df_cxp.columns and 'asignacion' in df_processed.columns:
        # Asegurarse de que la columna 'asignacion' esté limpia antes de la unión con CXP
        df_processed['asignacion'] = df_processed['asignacion'].astype(str).str.strip()
        df_processed = pd.merge(df_processed, df_cxp,
                                 left_on='asignacion', right_on='ref_hash',
                                 how='left', suffixes=('', '_cxp_merged'))
        st.success("✅ Datos unidos con **CXP**.")
    elif not df_cxp.empty:
        st.warning("⚠️ No se pudo unir con **CXP**. Verifique las columnas 'asignacion' o 'ref_hash'.")
    else:
        st.info("ℹ️ Omisión de unión con **CXP** (archivo no cargado o vacío).")

    st.success("🎉 ¡Todos los archivos han sido procesados y unidos!")
    # Reemplazar NaN/NaT en todo el DataFrame antes de guardar
    return df_processed.replace({np.nan: None, pd.NaT: None, '': None}) # También reemplazar cadenas vacías con None

def save_processed_data_to_supabase(df_to_save):
    """Guarda el DataFrame procesado en la tabla 'orders' de Supabase con validación y eliminación de duplicados."""
    if df_to_save.empty:
        st.warning("No hay datos procesados para guardar en Supabase.")
        return

    st.subheader("Paso 3: Guardando datos en Supabase...")

    # Obtener columnas válidas de Supabase
    valid_columns = get_valid_supabase_columns()
    
    if not valid_columns:
        st.error("❌ No se pudieron obtener las columnas válidas de Supabase. Abortando guardado.")
        return
    
    st.info(f"📋 Columnas válidas en Supabase: **{len(valid_columns)}** encontradas")
    
    if 'order_id_drapify' not in df_to_save.columns:
        st.error("Error crítico: La columna 'order_id_drapify' no se encuentra en el DataFrame final. Es necesaria para el seguimiento de órdenes. Abortando guardado.")
        return

    # Añadir timestamp de procesamiento de la aplicación
    df_to_save['processed_at_app'] = datetime.now()

    final_records_to_upload = []
    skipped_columns_info = set() # Para guardar qué columnas se omitieron
    used_columns_info = set()    # Para guardar qué columnas se usaron

    for index, row in df_to_save.iterrows():
        record = {}
        for df_col_name, db_col_name in supabase_db_schema_mapping.items():
            # Solo procesar si la columna existe en el DataFrame que estamos subiendo
            if df_col_name not in df_to_save.columns:
                continue
                
            # Solo incluir si la columna mapeada existe en Supabase
            if db_col_name not in valid_columns:
                skipped_columns_info.add(f"'{df_col_name}' (DF) -> '{db_col_name}' (DB)")
                continue
            
            used_columns_info.add(db_col_name)
            
            # Obtener y procesar el valor
            value = row.get(df_col_name)
            
            # Convertir valores nulos de Pandas a None de Python
            if pd.isna(value) or pd.isnull(value) or (isinstance(value, str) and value.strip() == ''):
                record[db_col_name] = None
            elif isinstance(value, datetime):
                record[db_col_name] = value.isoformat() # ISO 8601 para timestamps
            elif isinstance(value, (np.integer, int)):
                record[db_col_name] = int(value)
            elif isinstance(value, (np.floating, float)):
                record[db_col_name] = float(value)
            else:
                record[db_col_name] = str(value) # Asegurar que todo lo demás sea string

        final_records_to_upload.append(record)

    # Eliminar duplicados basados en 'order_id_drapify' para evitar insertar la misma orden dos veces
    seen_ids = set()
    unique_records = []
    duplicates_count = 0
    
    for record in final_records_to_upload:
        order_id = record.get('order_id_drapify')
        if order_id and order_id not in seen_ids:
            seen_ids.add(order_id)
            unique_records.append(record)
        else:
            duplicates_count += 1

    final_records_to_upload = unique_records

    # Mostrar información sobre el procesamiento y las columnas
    if skipped_columns_info:
        st.warning(f"⚠️ **Columnas omitidas** ({len(skipped_columns_info)}): No se encontraron en Supabase o en el DataFrame. {', '.join(sorted(list(skipped_columns_info)))}")
    
    if duplicates_count > 0:
        st.warning(f"⚠️ **Registros duplicados omitidos**: {duplicates_count} registros tenían 'order_id_drapify' duplicado y fueron ignorados.")
    
    st.info(f"✅ **Columnas que se intentarán insertar** ({len(used_columns_info)}): {', '.join(sorted(list(used_columns_info)))}")
    st.info(f"📊 **Registros únicos a insertar**: {len(final_records_to_upload)}")

    if not final_records_to_upload:
        st.info("No hay registros válidos y únicos para insertar después del filtrado.")
        return

    try:
        # Usar INSERT. Supabase manejará los IDs automáticamente si 'id' es autoincremental.
        # Esto evitará problemas con UPSERT si no se tiene una clave primaria definida para la deduplicación.
        response = supabase.table('orders').insert(final_records_to_upload).execute()

        if response.data:
            st.success(f"💾 **¡Datos guardados exitosamente!** Total de **{len(response.data)}** registros insertados en Supabase.")
        else:
            st.warning("⚠️ No se recibieron datos en la respuesta de Supabase, pero la operación pudo haber sido exitosa. Verifica la tabla directamente.")
            st.write(response) # Mostrar la respuesta completa para depuración
            
    except Exception as e:
        st.error(f"❌ **Error al guardar en Supabase**: {e}")
        st.exception(e) # Muestra el stack trace completo para depuración

---

# --- Páginas de la Aplicación ---
def page_process_files():
    st.markdown("<h1>📂 Cargar y Unir Archivos de Órdenes</h1>", unsafe_allow_html=True)
    st.info("Sube tus archivos de **Drapify (requerido)**, Anican Logistics, CXP y Aditionals. El sistema los procesará y unirá en Supabase.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Archivos Principales")
        drapify_file = st.file_uploader("📄 **DRAPIFY**", type=["csv", "xlsx", "xls"], key="drapify_uploader")
        anican_logistics_file = st.file_uploader("🚚 **Anican Logistics**", type=["csv", "xlsx", "xls"], key="anican_logistics_uploader")

    with col2:
        st.subheader("Archivos Auxiliares")
        cxp_file = st.file_uploader("🇨🇱 **Chile Express (CXP)**", type=["csv", "xlsx", "xls"], key="cxp_uploader")
        aditionals_file = st.file_uploader("➕ **Anican Aditionals**", type=["csv", "xlsx", "xls"], key="aditionals_uploader")

    st.markdown("---")
    
    # Botón para verificar columnas de Supabase
    if st.button("🔍 Verificar Columnas de Supabase"):
        st.info("Consultando la estructura de la tabla 'orders' en Supabase...")
        valid_columns = get_valid_supabase_columns()
        if valid_columns:
            st.success(f"✅ **{len(valid_columns)}** columnas encontradas en la tabla **'orders'** de Supabase.")
            st.json(valid_columns)
        else:
            st.error("❌ No se pudieron obtener las columnas de la tabla 'orders'. Asegúrate de que la tabla exista y haya al menos un registro.")
    
    if st.button("🚀 **Procesar y Guardar en Supabase**", type="primary"):
        if drapify_file:
            with st.spinner("Procesando archivos y uniendo datos... esto puede tardar un momento."):
                df_processed_global = process_files_for_upload(drapify_file, anican_logistics_file, cxp_file, aditionals_file)
                
                if not df_processed_global.empty:
                    st.session_state['df_processed'] = df_processed_global # Guardar en session_state si se necesita más tarde
                    
                    st.subheader("Vista Previa del DataFrame Consolidado (Primeras 10 filas)")
                    st.dataframe(df_processed_global.head(10), use_container_width=True)
                    st.write(f"**Total de registros procesados para subir**: **{len(df_processed_global)}**")

                    save_processed_data_to_supabase(df_processed_global)
                else:
                    st.error("⚠️ No se pudo procesar los archivos. Revisa los mensajes anteriores para más detalles.")
        else:
            st.warning("Por favor, **carga al menos el archivo DRAPIFY** para iniciar el procesamiento.")

---

def page_view_data():
    st.markdown("<h1>📊 Ver Datos Consolidados</h1>", unsafe_allow_html=True)
    st.info("Aquí puedes ver los datos que han sido cargados y consolidados en la base de datos de Supabase.")

    if st.button("🔄 **Recargar Datos Recientes**"):
        st.cache_data.clear() # Limpiar caché para obtener datos frescos
        st.rerun() # Volver a ejecutar la página para recargar

    try:
        # Limitar a 500 registros para evitar sobrecargar Streamlit con dataframes muy grandes
        response = supabase.table('orders').select('*').limit(500).order('created_at', desc=True).execute()
        
        if response.data:
            df_db = pd.DataFrame(response.data)
            st.success(f"✅ Se cargaron exitosamente **{len(df_db)}** registros de la base de datos.")
            st.dataframe(df_db, use_container_width=True)
        else:
            st.info("ℹ️ No hay datos en la tabla **'orders'** de Supabase aún. ¡Carga algunos desde la pestaña anterior!")
    except Exception as e:
        st.error(f"❌ Error al cargar datos desde Supabase: {e}")
        st.exception(e) # Mostrar detalles del error

---

def page_debug_schema():
    st.markdown("<h1>🔧 Depuración de Schema y Sistema</h1>", unsafe_allow_html=True)
    st.info("Utiliza esta sección para verificar la configuración de tu base de datos y el mapeo de columnas.")
    
    if st.button("🔍 **Verificar tabla 'orders' en Supabase**"):
        st.write("Intentando obtener la estructura de la tabla 'orders'...")
        valid_columns = get_valid_supabase_columns()
        if valid_columns:
            st.success(f"✅ La tabla **'orders'** existe en Supabase con **{len(valid_columns)}** columnas.")
            st.markdown("---")
            st.subheader("Columnas encontradas en Supabase:")
            # Mostrar columnas en formato de lista para mejor lectura
            for i, col in enumerate(sorted(valid_columns), 1):
                st.write(f"**{i}.** `{col}`")
        else:
            st.error("❌ La tabla **'orders'** no existe o está inaccesible en Supabase. Asegúrate de que los permisos sean correctos y la tabla esté creada.")
            st.info("Si la tabla es nueva, asegúrate de haberla creado y tener al menos un registro para que Supabase pueda inferir el esquema en `get_valid_supabase_columns`.")
            st.markdown("""
            **Sugerencia de SQL para crear la tabla 'orders' (ejemplo):**
            ```sql
            CREATE TABLE public.orders (
                id bigint GENERATED BY DEFAULT AS IDENTITY NOT NULL,
                created_at timestamp with time zone DEFAULT now() NOT NULL,
                order_id_drapify text NULL,
                system_hash text NULL,
                serial_hash text NULL,
                account_name text NULL,
                date_created text NULL,
                quantity_drapify numeric NULL,
                logistic_type text NULL,
                order_status_meli text NULL,
                etiqueta_envio text NULL,
                declare_value numeric NULL,
                net_real_amount numeric NULL,
                logistic_weight_lbs numeric NULL,
                refunded_date text NULL,
                order_number_anican text NULL,
                reference_anican text NULL,
                fob numeric NULL,
                insurance numeric NULL,
                logistics_anican numeric NULL,
                duties_prealert numeric NULL,
                duties_pay numeric NULL,
                duty_fee numeric NULL,
                saving numeric NULL,
                total_anican numeric NULL,
                external_id text NULL,
                date_cxp text NULL,
                ref_hash_cxp text NULL,
                co_aereo numeric NULL,
                arancel_cxp numeric NULL,
                iva_cxp numeric NULL,
                handling numeric NULL,
                dest_delivery numeric NULL,
                amt_due_cxp numeric NULL,
                goods_value numeric NULL,
                order_id_aditionals text NULL,
                quantity_aditionals numeric NULL,
                unit_price_aditionals numeric NULL,
                asignacion text NULL,
                processed_at_app timestamp with time zone NULL,
                CONSTRAINT orders_pkey PRIMARY KEY (id)
            );
            ALTER TABLE public.orders ENABLE ROW LEVEL SECURITY;
            ```
            """)

    st.subheader("Mapeo de Columnas Definido en la Aplicación")
    st.write("Este es el mapeo entre los nombres de columnas de tus archivos procesados y los nombres de columnas en la base de datos de Supabase:")
    
    # Crear un DataFrame para mostrar el mapeo de forma más legible
    mapping_df = pd.DataFrame(supabase_db_schema_mapping.items(), columns=['Columna en DataFrame Procesado', 'Columna en Supabase DB'])
    st.dataframe(mapping_df, use_container_width=True, hide_index=True)

    if st.button("📋 **Comparar Mapeo de la App vs. Columnas de Supabase**"):
        st.write("Realizando comparación...")
        valid_columns = get_valid_supabase_columns()
        if not valid_columns:
            st.error("❌ No se pudieron obtener las columnas de Supabase para comparar.")
            return
            
        missing_in_db = []
        present_in_db = []
        
        for df_col, db_col in supabase_db_schema_mapping.items():
            if db_col in valid_columns:
                present_in_db.append(db_col)
            else:
                missing_in_db.append(db_col)
            
        if missing_in_db:
            st.error(f"❌ **{len(missing_in_db)}** Columnas mapeadas en la aplicación **NO ENCONTRADAS** en la base de datos de Supabase:")
            for col in sorted(missing_in_db):
                st.write(f"• `{col}`")
            st.warning("Considera añadir estas columnas a tu tabla 'orders' en Supabase o revisar el mapeo.")
            
        if present_in_db:
            st.success(f"✅ **{len(present_in_db)}** Columnas mapeadas en la aplicación **ENCONTRADAS** en la base de datos de Supabase:")
            for col in sorted(present_in_db):
                st.write(f"• `{col}`")
        else:
            if not missing_in_db: # Only if there are no missing columns, but also no present ones
                st.info("Parece que no hay columnas mapeadas que coincidan con las de Supabase.")

    st.markdown("---")
    st.subheader("🛠️ Información del Sistema")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Estado de la conexión a Supabase:**")
        try:
            test_response = supabase.table('orders').select('id').limit(1).execute()
            st.success("✅ Conexión a Supabase **OK**")
        except Exception as e:
            st.error(f"❌ Error de conexión: {e}")
    
    with col2:
        st.write("**Formatos de Archivo Soportados:**")
        st.write("• CSV (`.csv`)")
        st.write("• Excel (`.xlsx`, `.xls`)")
        
    # Mostrar estadísticas de los datos
    if st.button("📊 **Obtener Estadísticas de Datos de la DB**"):
        try:
            # Contar registros totales
            count_response = supabase.table('orders').select('id', count='exact').execute()
            total_records = count_response.count if count_response.count is not None else 0
            
            # Obtener el timestamp del último procesamiento (si existe)
            recent_response = supabase.table('orders').select('processed_at_app').order('processed_at_app', desc=True).limit(1).execute()
            
            st.info(f"📈 **Total de registros en la base de datos**: **{total_records}**")
            
            if recent_response.data and recent_response.data[0]['processed_at_app']:
                st.info(f"📅 **Último procesamiento de datos en la aplicación**: **{datetime.fromisoformat(recent_response.data[0]['processed_at_app']).strftime('%Y-%m-%d %H:%M:%S')}**")
            else:
                st.info("ℹ️ No se encontró un registro de la última vez que los datos fueron procesados por la aplicación.")
            
        except Exception as e:
            st.error(f"Error al obtener estadísticas de la base de datos: {e}")
            st.exception(e)

---

# --- Lógica principal de la Aplicación Streamlit ---
st.sidebar.title("🏢 Sistema Contable Multi-País")
st.sidebar.markdown("---")

# Información del sistema en el sidebar
st.sidebar.markdown("### 📋 Estado del Sistema")
try:
    # Testear la conexión a Supabase
    test_connection = supabase.table('orders').select('id').limit(1).execute()
    st.sidebar.success("🟢 **Conectado a Supabase**")
except:
    st.sidebar.error("🔴 **Error de conexión a Supabase**")

st.sidebar.markdown("### 🎯 Fase Actual")
st.sidebar.info("**FASE 1:** Consolidación de Datos")
st.sidebar.markdown("""
**Objetivo:** Unificar datos de múltiples fuentes:
- 📄 **DRAPIFY** (base)
- 🚚 **Anican Logistics** - 🇨🇱 **Chile Express (CXP)**
- ➕ **Aditionals**
""")

st.sidebar.markdown("---")

page_selection = st.sidebar.radio("🧭 **Navegación:**", [
    "📂 Cargar y Unir Archivos", 
    "📊 Ver Datos Consolidados",
    "🔧 Depuración de Schema"
])

# Renderizar la página seleccionada
if page_selection == "📂 Cargar y Unir Archivos":
    page_process_files()
elif page_selection == "📊 Ver Datos Consolidados":
    page_view_data()
elif page_selection == "🔧 Depuración de Schema":
    page_debug_schema()

# Footer del sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("### 🚀 Próximamente")
st.sidebar.markdown("**FASE 2:** Fórmulas y Cálculos Financieros")
