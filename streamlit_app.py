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
        # Intenta cargar desde st.secrets.
        # Si estas líneas causan un KeyError, significa que st.secrets no está configurado.
        # En ese caso, se usarán las claves directamente codificadas (MENOS SEGURO PARA PRODUCCIÓN).
        url = st.secrets.get("supabase", {}).get("url", "https://qzexuqkedukcwcyhrpza.supabase.co")
        key = st.secrets.get("supabase", {}).get("key", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0")
        
        if not url or not key:
            st.error("Error: Las credenciales de Supabase (URL o KEY) no están configuradas correctamente en st.secrets o como fallback. La app no puede conectar.")
            st.stop() # Detiene la ejecución si las credenciales son inválidas

        return create_client(url, key)
    except Exception as e:
        st.error(f"Error crítico al inicializar Supabase: {e}. La aplicación se detendrá.")
        st.exception(e)
        st.stop() # Detiene la aplicación si hay un error crítico al conectar

supabase: Client = init_supabase_client()

# --- Función para obtener columnas válidas de Supabase ---
@st.cache_data(ttl=300)  # Cache por 5 minutos
def get_valid_supabase_columns():
    """Obtiene las columnas válidas de la tabla 'orders' en Supabase."""
    try:
        # Hacer una query simple para obtener la estructura
        result = supabase.table('orders').select('*').limit(1).execute()
        if result.data and len(result.data) > 0:
            return list(result.data[0].keys())
        else:
            # Si no hay datos, intentar con describe o information_schema
            return []
    except Exception as e:
        st.error(f"Error al obtener columnas de Supabase: {e}")
        return []

# --- Mapeo de columnas para Supabase (CORREGIDO) ---
# Este diccionario define cómo las columnas del DataFrame procesado (clave)
# se mapearán a los nombres de las columnas en tu tabla de Supabase (valor).
supabase_db_schema_mapping = {
    # Columnas de DRAPIFY (ya con nombres estandarizados)
    'system_hash': 'system_hash',
    'serial_hash': 'serial_hash',
    'order_id_drapify': 'order_id_drapify', # Usado como PK/Unique para upsert
    'account_name': 'account_name',
    'date_created': 'date_created',
    'quantity_drapify': 'quantity_drapify',
    'logistic_type': 'logistic_type',
    'order_status_meli': 'order_status_meli',
    'etiqueta_envio': 'etiqueta_envio',
    'declare_value': 'declare_value',
    'net_real_amount': 'net_real_amount',
    'logistic_weight_lbs': 'logistic_weight_lbs',
    'refunded_date': 'refunded_date',

    # Columnas de ANICAN LOGISTICS (ya con nombres estandarizados)
    'order_number_anican': 'order_number_anican',
    'reference_anican': 'reference_anican', # Clave de unión con Drapify
    'fob': 'fob',
    'insurance': 'insurance',
    'logistics_anican_value': 'logistics_anican',
    'duties_prealert': 'duties_prealert',
    'duties_pay': 'duties_pay',
    'duty_fee': 'duty_fee',
    'saving': 'saving',
    'total_anican_value': 'total_anican',
    'external_id_anican': 'external_id',

    # Columnas de CXP (CORREGIDAS)
    'date': 'date_cxp',
    'ref_hash': 'ref_hash_cxp', # Clave de unión con Asignacion
    'co_aereo': 'co_aereo',
    'arancel': 'arancel_cxp',
    'iva': 'iva_cxp',
    'handling': 'handling',
    'dest_delivery': 'dest_delivery',
    'amt_due': 'amt_due_cxp',  # CORREGIDO: era 'amt_due_cxp': 'amt_due_cxp'
    'goods_value': 'goods_value',

    # Columnas de ADITIONALS (ya con nombres estandarizados)
    'order_id_aditionals': 'order_id_aditionals', # Clave de unión con Anican Logistics
    'quantity_aditionals': 'quantity_aditionals',
    'unit_price_aditionals': 'unit_price_aditionals',

    # Columna calculada para unión
    'asignacion': 'asignacion',
    
    # Columna para registrar cuándo se subió/procesó el registro en la app
    'processed_at_app': 'processed_at_app' 
}

# --- Funciones de Utilidad ---
def leer_excel_o_csv(uploaded_file):
    """
    Lee un archivo Excel o CSV y lo devuelve como DataFrame,
    cargando todas las columnas como string inicialmente para robustez.
    """
    try:
        # Vuelve al inicio del archivo para asegurar una lectura completa cada vez
        uploaded_file.seek(0)
        if uploaded_file.name.endswith('.csv'):
            # Intenta detectar el delimitador automáticamente o usa la coma por defecto
            try:
                # Lee un fragmento para intentar detectar el delimitador
                sample = uploaded_file.getvalue().decode('utf-8').splitlines()[:5]
                sniffer = pd.io.common.DialectSniffer()
                dialect = sniffer.sniff(''.join(sample))
                uploaded_file.seek(0) # Vuelve al inicio del archivo
                return pd.read_csv(uploaded_file, dialect=dialect, dtype=str) # Leer todo como string inicialmente
            except Exception:
                uploaded_file.seek(0) # Vuelve al inicio del archivo
                return pd.read_csv(uploaded_file, dtype=str) # Prueba con delimitador por defecto
        elif uploaded_file.name.endswith(('.xlsx', '.xls')):
            return pd.read_excel(uploaded_file, dtype=str) # Leer todo como string inicialmente
        else:
            st.error(f"Formato de archivo no soportado: {uploaded_file.name}. Por favor, sube .csv, .xlsx o .xls.")
            return None
    except Exception as e:
        st.error(f"Error al leer el archivo {uploaded_file.name}: {e}")
        st.exception(e) # Muestra el traceback completo
        return None

def limpiar_nombres_columnas_generico(df):
    """Limpia los nombres de las columnas de un DataFrame a snake_case, sin renombrados específicos."""
    if df is None:
        return None
    new_columns = []
    for col in df.columns:
        cleaned_col = str(col).strip().replace('#', 'hash').replace(' ', '_').replace('.', '').replace('-', '_').lower()
        new_columns.append(cleaned_col)
    df.columns = new_columns
    return df

def try_read_cxp(uploaded_file):
    """
    Intenta leer el archivo CXP probando diferentes filas como encabezado
    y buscando columnas clave como 'Ref #' y 'Amt. Due'.
    """
    if uploaded_file is None:
        return None

    file_content = uploaded_file.getvalue() # Guarda el contenido para poder leerlo varias veces

    for header_row_index in range(5): # Probar encabezados en las filas 0 a 4 (índice 0-4)
        try:
            file_stream = io.BytesIO(file_content)
            if uploaded_file.name.endswith('.csv'):
                df_test = pd.read_csv(file_stream, header=header_row_index, dtype=str)
            else: # .xlsx o .xls
                df_test = pd.read_excel(file_stream, header=header_row_index, dtype=str)
            
            # --- RENOMBRADO EXPLÍCITO DE COLUMNAS CLAVE DE CXP AQUÍ ---
            # Esto es CRUCIAL para 'Ref #' y 'Amt. Due'
            cxp_renames = {
                'Ref #': 'ref_hash',
                'Amt. Due': 'amt_due',
                'Date': 'date', # También Date para consistencia
                'CO Aereo': 'co_aereo',
                'Arancel': 'arancel',
                'IVA': 'iva',
                'Handling': 'handling',
                'Dest. Delivery': 'dest_delivery',
                'Goods Value': 'goods_value',
            }
            # Aplica el renombrado solo si las columnas originales existen en df_test
            # Se usa una copia de df_test.columns para evitar modificar la lista mientras se itera
            current_cols_copy = df_test.columns.copy() 
            rename_map = {orig_name: new_name for orig_name, new_name in cxp_renames.items() if orig_name in current_cols_copy}
            df_test = df_test.rename(columns=rename_map)

            # --- AHORA APLICA LA LIMPIEZA GENÉRICA A TODO EL DATAFRAME ---
            df_test_cleaned = limpiar_nombres_columnas_generico(df_test.copy())

            # Busca columnas con nombres esperados (después del renombrado explícito y la limpieza genérica)
            ref_col_found = next((col for col in df_test_cleaned.columns if 'ref_hash' in col), None) # Ahora buscará 'ref_hash'
            amt_due_col_found = next((col for col in df_test_cleaned.columns if 'amt_due' in col), None) # Ahora buscará 'amt_due'

            if ref_col_found and amt_due_col_found:
                # Si las columnas clave se encuentran, devuelve el DataFrame ya renombrado y limpiado
                return df_test_cleaned
        except Exception as e:
            pass # Continúa intentando otros encabezados

    st.error("❌ No se pudieron encontrar las columnas clave ('Ref #' y 'Amt. Due') en las primeras 5 filas del archivo CXP. Por favor, verifica el formato.")
    return None

def process_files_for_upload(drapify_file, anican_logistics_file, cxp_file, aditionals_file):
    """
    Procesa y une todos los DataFrames de los archivos cargados,
    sin realizar cálculos complejos, solo preparación para la carga en Supabase.
    """
    df_drapify = None
    df_anican = None
    df_cxp = None
    df_aditionals = None

    st.subheader("Paso 1: Cargando y Limpiando Archivos Individuales")

    # --- Cargar DRAPIFY (archivo base) ---
    if drapify_file:
        df_drapify_raw = leer_excel_o_csv(drapify_file)
        if df_drapify_raw is not None:
            # --- RENOMBRADO EXPLÍCITO DE COLUMNAS CLAVE DE DRAPIFY AQUÍ ---
            drapify_renames = {
                'System#': 'System_Hash', 
                'Serial#': 'Serial_Hash', 
                'order_id': 'Order_ID', # Nombre original es order_id
                'account_name': 'Account_Name', # Nombre original es account_name
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
            # Aplica el renombrado solo si las columnas originales existen en df_drapify_raw
            current_cols_copy = df_drapify_raw.columns.copy() 
            rename_map = {orig_name: new_name for orig_name, new_name in drapify_renames.items() if orig_name in current_cols_copy}
            df_drapify = df_drapify_raw.rename(columns=rename_map)
            
            # --- AHORA APLICA LA LIMPIEZA GENÉRICA A TODO EL DATAFRAME ---
            df_drapify = limpiar_nombres_columnas_generico(df_drapify)

            # Convierte columnas numéricas *después* de renombrar y limpiar
            for col in ['quantity_drapify', 'declare_value', 'net_real_amount', 'logistic_weight_lbs']:
                if col in df_drapify.columns:
                    df_drapify[col] = pd.to_numeric(df_drapify[col], errors='coerce').fillna(0)
            # Asegura que las columnas de unión y cálculo de Asignacion sean string y limpias
            for col in ['order_id', 'serial_hash', 'account_name', 'order_status_meli', 'logistic_type']:
                 if col in df_drapify.columns:
                    df_drapify[col] = df_drapify[col].astype(str).str.strip()

            st.info("✅ Archivo DRAPIFY cargado y columnas limpias.")
        else:
            st.error("❌ No se pudo cargar el archivo DRAPIFY. Es un archivo base necesario.")
            return pd.DataFrame() # Retorna vacío si el base falla
    else:
        st.warning("⚠️ No se ha cargado el archivo DRAPIFY. No se puede continuar sin él.")
        return pd.DataFrame()

    # Iniciar el DataFrame procesado con Drapify. Renombramos order_id para el merge.
    df_processed = df_drapify.rename(columns={'order_id': 'order_id_drapify'}).copy()

    # --- Cargar ANICAN LOGISTICS ---
    if anican_logistics_file:
        df_anican_raw = leer_excel_o_csv(anican_logistics_file)
        if df_anican_raw is not None:
            # --- RENOMBRADO EXPLÍCITO DE COLUMNAS CLAVE DE ANICAN LOGISTICS AQUÍ ---
            anican_renames = {
                'Order number': 'Order_Number_Anican',
                'Reference': 'Reference_Anican', # Esta es la clave de unión con Drapify
                'FOB': 'FOB', 'Insurance': 'Insurance',
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
            # --- AHORA APLICA LA LIMPIEZA GENÉRICA A TODO EL DATAFRAME ---
            df_anican = limpiar_nombres_columnas_generico(df_anican)

            # Asegurar que las claves de unión y otras cols importantes sean string y limpias
            for col in ['order_number_anican', 'reference_anican', 'external_id_anican']:
                if col in df_anican.columns:
                    df_anican[col] = df_anican[col].astype(str).str.strip()
            # Convertir a numérico las columnas que deben serlo
            for col in ['fob', 'insurance', 'logistics_anican_value', 'duties_prealert', 'duties_pay', 'duty_fee', 'saving', 'total_anican_value']:
                if col in df_anican.columns:
                    df_anican[col] = pd.to_numeric(df_anican[col], errors='coerce').fillna(0)

            st.info("✅ Archivo Anican Logistics cargado y columnas limpias.")

    # --- Cargar ADITIONALS ---
    if aditionals_file:
        df_aditionals_raw = leer_excel_o_csv(aditionals_file)
        if df_aditionals_raw is not None:
            # --- RENOMBRADO EXPLÍCITO DE COLUMNAS CLAVE DE ADITIONALS AQUÍ ---
            aditionals_renames = {
                'Order Id': 'Order_ID_Aditionals', # Esta es la clave de unión
                'Quantity': 'Quantity_Aditionals',
                'UnitPrice': 'Unit_Price_Aditionals',
            }
            current_cols_copy = df_aditionals_raw.columns.copy() 
            rename_map = {orig_name: new_name for orig_name, new_name in aditionals_renames.items() if orig_name in current_cols_copy}
            df_aditionals = df_aditionals_raw.rename(columns=rename_map)
            # --- AHORA APLICA LA LIMPIEZA GENÉRICA A TODO EL DATAFRAME ---
            df_aditionals = limpiar_nombres_columnas_generico(df_aditionals)

            if 'order_id_aditionals' in df_aditionals.columns:
                df_aditionals['order_id_aditionals'] = df_aditionals['order_id_aditionals'].astype(str).str.strip()
            for col in ['quantity_aditionals', 'unit_price_aditionals']:
                if col in df_aditionals.columns:
                    df_aditionals[col] = pd.to_numeric(df_aditionals[col], errors='coerce').fillna(0)
            st.info("✅ Archivo Aditionals cargado y columnas limpias.")

    # --- Cargar CXP ---
    if cxp_file:
        # try_read_cxp ya realiza el renombrado explícito y la limpieza genérica
        df_cxp = try_read_cxp(cxp_file) 
        if df_cxp is not None:
            # Aquí df_cxp ya debería tener 'ref_hash', 'amt_due' y estar limpiado genéricamente
            # Solo asegurar que la clave de unión sea string y limpia
            if 'ref_hash' in df_cxp.columns: # Debe ser 'ref_hash' después del try_read_cxp
                df_cxp['ref_hash'] = df_cxp['ref_hash'].astype(str).str.strip()
            for col in ['co_aereo', 'arancel', 'iva', 'handling', 'dest_delivery', 'amt_due', 'goods_value']:
                if col in df_cxp.columns:
                    df_cxp[col] = pd.to_numeric(df_cxp[col], errors='coerce').fillna(0)
            st.info("✅ Archivo CXP cargado y columnas limpias.")


    st.subheader("Paso 2: Uniendo DataFrames")

    # 1. Unir Drapify con Anican Logistics (usando order_id_drapify y reference_anican)
    # Las columnas para merge deben existir y estar en el formato correcto (str)
    if df_anican is not None and 'reference_anican' in df_anican.columns and 'order_id_drapify' in df_processed.columns:
        df_processed = pd.merge(df_processed, df_anican,
                                left_on='order_id_drapify', right_on='reference_anican',
                                how='left', suffixes=('', '_anican_logistics')) # Sufijo
        st.success("✅ Drapify unido con Anican Logistics.")
    else:
        st.warning("⚠️ No se pudo unir Drapify con Anican Logistics. Se omitirá esta unión.")

    # 2. Unir con Aditionals (usando order_number_anican y order_id_aditionals)
    # La columna 'order_number_anican' debe venir de la unión previa con Anican Logistics.
    if df_aditionals is not None and 'order_id_aditionals' in df_aditionals.columns:
        if 'order_number_anican' in df_processed.columns: # Asegurarse de que la columna existe en el DF procesado
            df_processed = pd.merge(df_processed, df_aditionals,
                                    left_on='order_number_anican', right_on='order_id_aditionals',
                                    how='left', suffixes=('', '_aditionals_merged')) # Sufijo
            st.success("✅ Datos unidos con Aditionals.")
        else:
            st.warning("⚠️ La columna 'order_number_anican' no se encontró en el DF principal (puede que Anican Logistics no se haya cargado o unido correctamente). No se puede unir con Aditionals.")
    else:
        st.warning("⚠️ No se pudo unir con Aditionals. Se omitirá esta unión.")

    # 3. Calcular columna 'Asignacion' (necesaria para la unión con CXP)
    df_processed['asignacion'] = None # Inicializar
    if 'account_name' in df_processed.columns and 'serial_hash' in df_processed.columns:
        # Asegurar que serial_hash y account_name sean string y limpiar espacios
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
        st.success("✅ Columna 'Asignacion' calculada.")
    else:
        st.warning("⚠️ No se pudo calcular 'Asignacion'. Faltan columnas clave ('account_name' o 'serial_hash') en DRAPIFY. Revisa tu archivo DRAPIFY.")


    # 4. Unir con CXP (usando asignacion y ref_hash)
    if df_cxp is not None and 'ref_hash' in df_cxp.columns and 'asignacion' in df_processed.columns: # 'ref_hash' ya existe después del try_read_cxp
        # Asegurar que la columna 'asignacion' sea de tipo string y sin espacios, ya que se usa como clave de unión
        df_processed['asignacion'] = df_processed['asignacion'].astype(str).str.strip()
        df_processed = pd.merge(df_processed, df_cxp,
                                left_on='asignacion', right_on='ref_hash', # Ahora usamos 'ref_hash' de df_cxp
                                how='left', suffixes=('', '_cxp_merged')) # Sufijo
        st.success("✅ Datos unidos con CXP.")
    else:
        st.warning("⚠️ No se pudo unir con CXP. Se omitirá esta unión (asegúrate de que CXP tenga 'Ref #' y que 'Asignacion' se haya calculado correctamente).")

    st.success("🎉 ¡Todos los archivos han sido procesados y unidos en un solo DataFrame!")
    # Reemplaza todos los NaN y NaT con None para una mejor inserción en Supabase
    return df_processed.replace({np.nan: None, pd.NaT: None}) 

def save_processed_data_to_supabase(df_to_save):
    """Guarda el DataFrame procesado en la tabla 'orders' de Supabase con validación de columnas."""
    if df_to_save.empty:
        st.warning("No hay datos procesados para guardar en Supabase.")
        return

    st.subheader("Paso 3: Guardando datos en Supabase...")

    # Obtener columnas válidas de Supabase
    valid_columns = get_valid_supabase_columns()
    
    if not valid_columns:
        st.error("❌ No se pudieron obtener las columnas válidas de Supabase. Verifica la conexión y que la tabla 'orders' exista.")
        return
    
    st.info(f"📋 Columnas válidas en Supabase: {', '.join(valid_columns)}")
    
    # Asegúrate de que 'order_id_drapify' esté en el DataFrame a guardar para el on_conflict
    if 'order_id_drapify' not in df_to_save.columns:
        st.error("Error crítico: 'order_id_drapify' no se encuentra en el DataFrame final. No se puede guardar en Supabase sin una clave única.")
        return

    # Añadir un timestamp de la aplicación para cada registro
    df_to_save['processed_at_app'] = datetime.now()

    final_records_to_upload = []
    skipped_columns = set()
    used_columns = set()

    for _, row in df_to_save.iterrows():
        record = {}
        for df_col_name, db_col_name in supabase_db_schema_mapping.items():
            # Solo procesar si la columna existe en el DataFrame
            if df_col_name not in df_to_save.columns:
                continue
                
            # Solo incluir si la columna existe en Supabase
            if db_col_name not in valid_columns:
                skipped_columns.add(f"{df_col_name} -> {db_col_name}")
                continue
            
            used_columns.add(db_col_name)
            
            # Obtiene el valor de la fila
            value = row.get(df_col_name) 
            
            # Reemplazar NaN/NaT con None y convertir tipos básicos para Supabase
            if pd.isna(value) or pd.isnull(value):
                record[db_col_name] = None
            elif isinstance(value, datetime):
                record[db_col_name] = value.isoformat()
            elif isinstance(value, (np.integer, int)):
                record[db_col_name] = int(value)
            elif isinstance(value, (np.floating, float)):
                record[db_col_name] = float(value)
            else:
                record[db_col_name] = str(value)

        final_records_to_upload.append(record)

    # Mostrar información sobre columnas procesadas
    if skipped_columns:
        st.warning(f"⚠️ Columnas omitidas (no existen en Supabase): {', '.join(sorted(skipped_columns
