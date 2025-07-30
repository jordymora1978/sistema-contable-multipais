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

# --- Mapeo de columnas para Supabase ---
# Este diccionario define cómo las columnas del DataFrame procesado (clave)
# se mapearán a los nombres de las columnas en tu tabla de Supabase (valor).
# ES CRUCIAL QUE LOS VALORES (nombres de la DB) COINCIDAN EXACTAMENTE CON TU ESQUEMA EN SUPABASE.
supabase_db_schema_mapping = {
    # Columnas de DRAPIFY (nombres ya limpiados/renombrados)
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

    # Columnas de ANICAN LOGISTICS (nombres ya limpiados/renombrados)
    'order_number_anican': 'order_number_anican',
    'reference_anican': 'reference_anican', # Clave de unión con Drapify
    'fob': 'fob',
    'insurance': 'insurance',
    'logistics_anican': 'logistics_anican',
    'duties_prealert': 'duties_prealert',
    'duties_pay': 'duties_pay',
    'duty_fee': 'duty_fee',
    'saving': 'saving',
    'total_anican': 'total_anican',
    'external_id': 'external_id',

    # Columnas de CXP (nombres ya limpiados/renombrados)
    'date_cxp': 'date_cxp',
    'ref_hash_cxp': 'ref_hash_cxp', # Clave de unión con Asignacion
    'co_aereo': 'co_aereo',
    'arancel_cxp': 'arancel_cxp',
    'iva_cxp': 'iva_cxp',
    'handling': 'handling',
    'dest_delivery': 'dest_delivery',
    'amt_due_cxp': 'amt_due_cxp',
    'goods_value': 'goods_value',

    # Columnas de ADITIONALS (nombres ya limpiados/renombrados)
    'order_id_aditionals': 'order_id_aditionals', # Clave de unión con Anican Logistics
    'quantity_aditionals': 'quantity_aditionals',
    'unit_price_aditionals': 'unit_price_aditionals',

    # Columna calculada para unión
    'asignacion': 'asignacion',
    
    # Columna para registrar cuándo se cargó/procesó el registro en la app
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

def limpiar_nombres_columnas(df):
    """Limpia los nombres de las columnas de un DataFrame para hacerlos más consistentes (snake_case)."""
    if df is None:
        return None
    new_columns = []
    for col in df.columns:
        # Convierte a string, elimina espacios en blanco al inicio/final, reemplaza # por hash,
        # espacios por _, puntos por nada, guiones por _, y todo a minúsculas.
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
            # Crea un objeto io.BytesIO para cada intento de lectura, así el archivo no se "agota"
            file_stream = io.BytesIO(file_content)

            if uploaded_file.name.endswith('.csv'):
                df_test = pd.read_csv(file_stream, header=header_row_index, dtype=str)
            else: # .xlsx o .xls
                df_test = pd.read_excel(file_stream, header=header_row_index, dtype=str)

            df_test_cleaned = limpiar_nombres_columnas(df_test.copy()) # Limpiar una copia para la verificación

            # Busca columnas con nombres esperados (después de limpiar)
            ref_col_found = next((col for col in df_test_cleaned.columns if 'ref_hash' in col or 'ref_num' in col or 'ref_' in col), None)
            amt_due_col_found = next((col for col in df_test_cleaned.columns if 'amt_due' in col or 'amount_due' in col), None)

            if ref_col_found and amt_due_col_found:
                st.info(f"✅ Archivo CXP leído exitosamente con encabezado en la fila {header_row_index + 1}.")
                return limpiar_nombres_columnas(df_test) # Devuelve el DataFrame original con las columnas limpias
        except Exception as e:
            # st.info(f"Intento de encabezado en fila {header_row_index + 1} falló para CXP: {e}")
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

    # --- Cargar Drapify (archivo base) ---
    if drapify_file:
        df_drapify = leer_excel_o_csv(drapify_file)
        if df_drapify is not None:
            df_drapify = limpiar_nombres_columnas(df_drapify)
            # Renombrar columnas de Drapify a los nombres estandarizados/mapeados
            df_drapify = df_drapify.rename(columns={
                'order_id': 'order_id_drapify',
                'quantity': 'quantity_drapify',
                'system_hash': 'system_hash', # System# -> system_hash
                'serial_hash': 'serial_hash', # Serial# -> serial_hash
                'etiqueta_envio': 'etiqueta_envio', # ETIQUETA_ENVIO
                'declare_value': 'declare_value', # Declare Value
                'net_real_amount': 'net_real_amount',
                'logistic_weight_lbs': 'logistic_weight_lbs',
                'refunded_date': 'refunded_date',
                'account_name': 'account_name',
                'date_created': 'date_created',
                'logistic_type': 'logistic_type',
                'order_status_meli': 'order_status_meli',
            })
            # Asegurar que las columnas clave para uniones sean de tipo string y limpiar espacios
            for col in ['order_id_drapify', 'serial_hash', 'account_name', 'date_created',
                        'logistic_type', 'order_status_meli', 'etiqueta_envio', 'refunded_date']:
                if col in df_drapify.columns:
                    df_drapify[col] = df_drapify[col].astype(str).str.strip()
            # Convertir columnas numéricas que se usarán como tal (solo las de Drapify aquí)
            for col in ['quantity_drapify', 'declare_value', 'net_real_amount', 'logistic_weight_lbs']:
                if col in df_drapify.columns:
                    df_drapify[col] = pd.to_numeric(df_drapify[col], errors='coerce').fillna(0)

            st.info("✅ Archivo DRAPIFY cargado y columnas limpias.")
        else:
            st.error("❌ No se pudo cargar el archivo DRAPIFY. Es un archivo base necesario.")
            return pd.DataFrame() # Retorna vacío si el base falla
    else:
        st.warning("⚠️ No se ha cargado el archivo DRAPIFY. No se puede continuar sin él.")
        return pd.DataFrame()

    df_processed = df_drapify.copy() # Iniciamos con Drapify

    # --- Cargar Anican Logistics ---
    if anican_logistics_file:
        df_anican = leer_excel_o_csv(anican_logistics_file)
        if df_anican is not None:
            df_anican = limpiar_nombres_columnas(df_anican)
            # Renombrar columnas de Anican Logistics
            df_anican = df_anican.rename(columns={
                'order_number': 'order_number_anican',
                'reference': 'reference_anican',
                'logistics': 'logistics_anican',
                'total': 'total_anican',
                # Asegura que el resto de columnas estén en el mapeo si no se renombran
                'fob': 'fob', 'insurance': 'insurance', 'duties_prealert': 'duties_prealert',
                'duties_pay': 'duties_pay', 'duty_fee': 'duty_fee', 'saving': 'saving', 'external_id': 'external_id',
            })
            # Convertir a string y limpiar espacios para las columnas de texto/ID
            for col in ['order_number_anican', 'reference_anican', 'external_id']:
                if col in df_anican.columns:
                    df_anican[col] = df_anican[col].astype(str).str.strip()
            # Convertir columnas numéricas
            for col in ['fob', 'insurance', 'logistics_anican', 'duties_prealert', 'duties_pay', 'duty_fee', 'saving', 'total_anican']:
                if col in df_anican.columns:
                    df_anican[col] = pd.to_numeric(df_anican[col], errors='coerce').fillna(0)

            st.info("✅ Archivo Anican Logistics cargado y columnas limpias.")

    # --- Cargar Aditionals ---
    if aditionals_file:
        df_aditionals = leer_excel_o_csv(aditionals_file)
        if df_aditionals is not None:
            df_aditionals = limpiar_nombres_columnas(df_aditionals)
            # Renombrar columnas de Aditionals
            df_aditionals = df_aditionals.rename(columns={
                'order_id': 'order_id_aditionals',
                'quantity': 'quantity_aditionals',
                'unit_price': 'unit_price_aditionals',
            })
            # Convertir a string y limpiar espacios para la columna de ID
            if 'order_id_aditionals' in df_aditionals.columns:
                df_aditionals['order_id_aditionals'] = df_aditionals['order_id_aditionals'].astype(str).str.strip()
            # Convertir columnas numéricas
            for col in ['quantity_aditionals', 'unit_price_aditionals']:
                if col in df_aditionals.columns:
                    df_aditionals[col] = pd.to_numeric(df_aditionals[col], errors='coerce').fillna(0)
            st.info("✅ Archivo Aditionals cargado y columnas limpias.")

    # --- Cargar CXP ---
    if cxp_file:
        df_cxp = try_read_cxp(cxp_file) # Usa la función robusta
        if df_cxp is not None:
            # Renombrar columnas de CXP
            df_cxp = df_cxp.rename(columns={
                'ref_hash': 'ref_hash_cxp', # Ref # -> ref_hash_cxp (nombre limpio)
                'date': 'date_cxp',
                'co_aereo': 'co_aereo',
                'arancel': 'arancel_cxp',
                'iva': 'iva_cxp',
                'handling': 'handling',
                'dest_delivery': 'dest_delivery',
                'amt_due': 'amt_due_cxp',
                'goods_value': 'goods_value',
            })
            # Convertir a string y limpiar espacios para la columna de ID
            if 'ref_hash_cxp' in df_cxp.columns:
                df_cxp['ref_hash_cxp'] = df_cxp['ref_hash_cxp'].astype(str).str.strip()
            # Convertir columnas numéricas
            for col in ['co_aereo', 'arancel_cxp', 'iva_cxp', 'handling', 'dest_delivery', 'amt_due_cxp', 'goods_value']:
                if col in df_cxp.columns:
                    df_cxp[col] = pd.to_numeric(df_cxp[col], errors='coerce').fillna(0)
            st.info("✅ Archivo CXP cargado y columnas limpias.")


    st.subheader("Paso 2: Uniendo DataFrames")

    # 1. Unir Drapify con Anican Logistics (usando order_id_drapify y reference_anican)
    if df_anican is not None and 'reference_anican' in df_anican.columns and 'order_id_drapify' in df_processed.columns:
        df_processed = pd.merge(df_processed, df_anican,
                                left_on='order_id_drapify', right_on='reference_anican',
                                how='left', suffixes=('', '_anican_logistics_suffix')) # Sufijo para evitar conflictos
        st.success("✅ Drapify unido con Anican Logistics.")
    else:
        st.warning("⚠️ No se pudo unir Drapify con Anican Logistics. Se omitirá esta unión.")

    # 2. Unir con Aditionals (usando order_number_anican y order_id_aditionals)
    # df_processed ya tiene las columnas de Anican Logistics (si se unió)
    if df_aditionals is not None and 'order_id_aditionals' in df_aditionals.columns:
        # La columna 'order_number_anican' debe venir de la unión previa con Anican Logistics
        if 'order_number_anican' in df_processed.columns:
            df_processed = pd.merge(df_processed, df_aditionals,
                                    left_on='order_number_anican', right_on='order_id_aditionals',
                                    how='left', suffixes=('', '_aditionals_merged_suffix')) # Sufijo para evitar conflictos
            st.success("✅ Datos unidos con Aditionals.")
        else:
            st.warning("⚠️ La columna 'order_number_anican' no se encontró en el DataFrame principal (puede que Anican Logistics no se haya cargado o unido correctamente). No se puede unir con Aditionals. Asegúrate de cargar Anican Logistics.")
    else:
        st.warning("⚠️ No se pudo unir con Aditionals. Se omitirá esta unión.")

    # 3. Calcular columna 'Asignacion' (necesaria para la unión con CXP)
    df_processed['asignacion'] = None # Inicializar
    if 'account_name' in df_processed.columns and 'serial_hash' in df_processed.columns:
        # Asegurar que serial_hash sea string y limpiar espacios para la concatenación
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
        st.warning("⚠️ No se pudo calcular 'Asignacion'. Faltan columnas clave ('account_name' o 'serial_hash') en DRAPIFY después de la limpieza. Revisa tu archivo DRAPIFY.")


    # 4. Unir con CXP (usando asignacion y ref_hash_cxp)
    if df_cxp is not None and 'ref_hash_cxp' in df_cxp.columns and 'asignacion' in df_processed.columns:
        # Asegurar que la columna 'asignacion' sea de tipo string y sin espacios, ya que se usa como clave de unión
        df_processed['asignacion'] = df_processed['asignacion'].astype(str).str.strip()
        df_processed = pd.merge(df_processed, df_cxp,
                                left_on='asignacion', right_on='ref_hash_cxp',
                                how='left', suffixes=('', '_cxp_merged_suffix')) # Sufijo para evitar conflictos
        st.success("✅ Datos unidos con CXP.")
    else:
        st.warning("⚠️ No se pudo unir con CXP. Se omitirá esta unión (asegúrate de que CXP tenga 'Ref #' y que 'Asignacion' se haya calculado correctamente).")

    st.success("🎉 ¡Todos los archivos han sido procesados y unidos en un solo DataFrame!")
    # Reemplaza todos los NaN y NaT con None para una mejor inserción en Supabase
    return df_processed.replace({np.nan: None, pd.NaT: None}) 

def save_processed_data_to_supabase(df_to_save):
    """Guarda el DataFrame procesado en la tabla 'orders_data_raw' de Supabase."""
    if df_to_save.empty:
        st.warning("No hay datos procesados para guardar en Supabase.")
        return

    st.subheader("Paso 3: Guardando datos en Supabase...")

    final_records_to_upload = []
    
    # Asegúrate de que order_id_drapify esté en el DataFrame a guardar para el on_conflict
    if 'order_id_drapify' not in df_to_save.columns:
        st.error("Error crítico: 'order_id_drapify' no se encuentra en el DataFrame final. No se puede guardar en Supabase sin una clave única.")
        return

    # Añadir un timestamp de la aplicación para cada registro si aún no existe en el DF final
    df_to_save['processed_at_app'] = datetime.now()


    for _, row in df_to_save.iterrows():
        record = {}
        for df_col_name, db_col_name in supabase_db_schema_mapping.items():
            # Obtiene el valor de la fila. Si la columna no existe en el DF, .get() devuelve None por defecto.
            value = row.get(df_col_name) 
            
            # Reemplazar NaN/NaT con None y convertir tipos básicos para Supabase
            if pd.isna(value) or pd.isnull(value): # Verifica tanto para np.nan como pd.NaT
                record[db_col_name] = None
            elif isinstance(value, datetime):
                record[db_col_name] = value.isoformat() # Formato ISO para timestamps de DB
            elif isinstance(value, (np.integer, int)):
                record[db_col_name] = int(value)
            elif isinstance(value, (np.floating, float)):
                record[db_col_name] = float(value)
            else:
                record[db_col_name] = str(value) # Por defecto, convertir a string

        final_records_to_upload.append(record)

    try:
        # Usa el nombre de tabla que ya tienes en Supabase, por ejemplo 'orders_data_raw'
        # Asegúrate de que 'order_id_drapify' es la clave primaria o única en tu tabla 'orders_data_raw'
        # para que el upsert funcione correctamente.
        response = supabase.table('orders_data_raw').upsert(final_records_to_upload, on_conflict='order_id_drapify').execute()

        if response.data:
            st.success(f"💾 ¡Datos guardados exitosamente en Supabase! Total de {len(response.data)} registros insertados/actualizados.")
            # st.json(response.data) # Descomentar para ver la respuesta detallada de Supabase si es necesario
        else:
            st.warning("⚠️ No se recibieron datos en la respuesta de Supabase. Esto podría indicar un problema, aunque los registros se hayan procesado.")
            st.write(response) # Muestra la respuesta completa para depuración
            
    except Exception as e:
        st.error(f"❌ Ocurrió un error al guardar los datos en Supabase: {e}")
        st.exception(e) # Muestra el stack trace del error

# --- Páginas de la Aplicación Streamlit ---
def page_process_files():
    st.markdown("<h1>📂 Cargar y Unir Archivos de Órdenes</h1>", unsafe_allow_html=True)
    st.info("Sube tus archivos de Drapify, Anican Logistics, CXP y Aditionals. El sistema los leerá, limpiará y unirá toda la información en una única tabla en Supabase.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Archivos Principales")
        drapify_file = st.file_uploader("📄 DRAPIFY (Orders_XXXXXXXX)", type=["csv", "xlsx", "xls"], help="Archivo principal con las órdenes de MercadoLibre.", key="drapify_uploader")
        anican_logistics_file = st.file_uploader("🚚 Anican Logistics", type=["csv", "xlsx", "xls"], help="Archivo con costos logísticos de Anican.", key="anican_logistics_uploader")

    with col2:
        st.subheader("Archivos Auxiliares")
        cxp_file = st.file_uploader("🇨🇱 Chile Express (CXP)", type=["csv", "xlsx", "xls"], help="Archivo de Chile Express con costos logísticos y de aduana.", key="cxp_uploader")
        aditionals_file = st.file_uploader("➕ Anican Aditionals", type=["csv", "xlsx", "xls"], help="Archivo con costos adicionales de Anican.", key="aditionals_uploader")

    st.markdown("---")
    if st.button("🚀 Procesar y Guardar en Supabase", type="primary"):
        if drapify_file: # Drapify es el mínimo requerido para que el proceso base inicie
            with st.spinner("Procesando y uniendo archivos... Esto puede tomar unos segundos."):
                df_processed_global = process_files_for_upload(drapify_file, anican_logistics_file, cxp_file, aditionals_file)
                
                if not df_processed_global.empty:
                    st.session_state['df_processed'] = df_processed_global # Guarda en session_state por si se necesita después
                    
                    st.subheader("Vista Previa de Datos Unidos (Primeras 10 filas)")
                    st.dataframe(df_processed_global.head(10), use_container_width=True)
                    st.write(f"Total de registros unidos: {len(df_processed_global)}")

                    # Guarda en Supabase
                    save_processed_data_to_supabase(df_processed_global)
                else:
                    st.error("⚠️ No se pudo procesar los archivos. Revisa los mensajes de error/advertencia anteriores.")
        else:
            st.warning("Por favor, carga al menos el archivo DRAPIFY para iniciar el procesamiento.")

def page_view_data():
    st.markdown("<h1>📊 Ver Datos Consolidados en Supabase</h1>", unsafe_allow_html=True)
    st.info("Aquí puedes revisar los datos que han sido cargados y consolidados en tu base de datos Supabase.")

    if st.button("🔄 Cargar Datos Recientes de Supabase"):
        st.cache_data.clear() # Limpia la caché para obtener los datos más recientes
        st.rerun() # Recarga la página para mostrar los datos actualizados

    try:
        # Asegúrate de que esta sea el nombre correcto de tu tabla en Supabase
        # 'orders_data_raw' es la tabla donde se guardan los datos consolidados.
        response = supabase.table('orders_data_raw').select('*').limit(500).order('order_id_drapify', desc=True).execute() 
        
        if response.data:
            df_db = pd.DataFrame(response.data)
            st.success(f"✅ Se cargaron {len(df_db)} registros de Supabase.")
            st.dataframe(df_db, use_container_width=True)
        else:
            st.info("ℹ️ No hay datos disponibles en la tabla 'orders_data_raw' de Supabase aún. ¡Carga algunos archivos primero!")
    except Exception as e:
        st.error(f"❌ Error al cargar datos de Supabase: {e}")
        st.warning("Asegúrate de que la tabla 'orders_data_raw' exista en tu base de datos Supabase y que las claves de acceso sean correctas.")


# --- Lógica principal de la aplicación ---
st.sidebar.title("Navegación del Sistema")
page_selection = st.sidebar.radio("Elige una opción:", ["📂 Cargar y Unir Archivos", "📊 Ver Datos Consolidados"])

if page_selection == "📂 Cargar y Unir Archivos":
    page_process_files()
elif page_selection == "📊 Ver Datos Consolidados":
    page_view_data()
