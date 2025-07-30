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
        return [
            'id', 'created_at', 'system_hash', 'serial_hash', 'order_id', 'pack_id', 'asin',
            'client_first_name', 'client_last_name', 'client_doc_id', 'account_name', 
            'date_created', 'quantity', 'title', 'unit_price', 'logistic_type', 'address_line',
            'street_name', 'street_number', 'city', 'state', 'country', 'receiver_phone',
            'amz_order_id', 'prealert_id', 'etiqueta_envio', 'order_status_meli', 'declare_value',
            'meli_fee', 'iva', 'ica', 'fuente', 'senders_cost', 'gross_amount', 'net_received_amount',
            'nombre_del_tercero', 'direccion', 'apelido_del_tercero', 'estado', 'razon_social',
            'ciudad', 'numero_de_documento', 'digital_verification', 'tipo', 'telefono', 'giro',
            'correo', 'net_real_amount', 'logistic_weight_lbs', 'refunded_date',
            # Columnas de Logistic Anicam
            'guide_number', 'order_number', 'reference', 'sap_code', 'invoice_logistic', 'status',
            'fob', 'unit', 'weight', 'length', 'width', 'height', 'insurance', 'logistics',
            'duties_prealert', 'duties_pay', 'duty_fee', 'saving', 'total', 'description',
            'shipper', 'phone_shipper', 'consignee', 'identification', 'country_logistic',
            'state_logistic', 'city_logistic', 'address_logistic', 'phone_consignee',
            'master_guide', 'tariff_position', 'external_id', 'invoice_anicam',
            # Columnas de Aditionals
            'order_id_aditionals', 'item', 'reference_aditionals', 'description_aditionals',
            'quantity_aditionals', 'unit_price_aditionals', 'total_aditionals',
            # Columna calculada
            'asignacion',
            # Columnas de CXP
            'ot_number', 'date_cxp', 'ref_hash', 'consignee_cxp', 'co_aereo', 'arancel',
            'iva_cxp', 'handling', 'dest_delivery', 'amt_due', 'goods_value',
            # Metadatos
            'processed_at_app'
        ]
        
    except Exception as e:
        st.error(f"Error al obtener columnas de Supabase: {e}")
        return []

# --- Funciones de Utilidad ---
def leer_excel_o_csv(uploaded_file):
    """Lee un archivo Excel o CSV y lo devuelve como DataFrame."""
    try:
        uploaded_file.seek(0)
        if uploaded_file.name.endswith('.csv'):
            try:
                # Intentar diferentes encodings para CSVs
                encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
                for encoding in encodings:
                    try:
                        uploaded_file.seek(0)
                        return pd.read_csv(uploaded_file, encoding=encoding, dtype=str)
                    except UnicodeDecodeError:
                        continue
                # Si todos los encodings fallan, usar el último intento
                uploaded_file.seek(0)
                return pd.read_csv(uploaded_file, dtype=str)
            except Exception:
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

def limpiar_nombres_columnas(df):
    """Limpia los nombres de las columnas de un DataFrame a snake_case y remueve caracteres especiales."""
    if df is None:
        return None
    new_columns = []
    for col in df.columns:
        # Convertir a string, quitar espacios, reemplazar caracteres especiales, y a minúsculas
        cleaned_col = str(col).strip()
        cleaned_col = cleaned_col.replace('#', '_hash')
        cleaned_col = cleaned_col.replace(' ', '_')
        cleaned_col = cleaned_col.replace('.', '')
        cleaned_col = cleaned_col.replace('-', '_')
        cleaned_col = cleaned_col.replace('(', '')
        cleaned_col = cleaned_col.replace(')', '')
        cleaned_col = cleaned_col.lower()
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
            file_stream = io.BytesIO(file_content)
            if uploaded_file.name.endswith('.csv'):
                df_test = pd.read_csv(file_stream, header=header_row_index, dtype=str)
            else:
                df_test = pd.read_excel(file_stream, header=header_row_index, dtype=str)
            
            df_test_cleaned = limpiar_nombres_columnas(df_test.copy())
            
            # Verificar si las columnas clave existen después de la limpieza
            has_ref = any('ref' in col for col in df_test_cleaned.columns)
            has_amt_due = any('amt' in col and 'due' in col for col in df_test_cleaned.columns)

            if has_ref and has_amt_due:
                st.info(f"✨ Encabezado del archivo CXP detectado en la fila: **{header_row_index + 1}**")
                return df_test_cleaned
        except Exception:
            pass

    st.error("❌ No se pudieron encontrar las columnas clave en el archivo CXP. Por favor, verifica el formato.")
    return None

def process_files_according_to_prompt(drapify_file, logistic_file, aditionals_file, cxp_file):
    """
    Procesa archivos siguiendo EXACTAMENTE las especificaciones del prompt.
    
    PASO 1: Base del Consolidado - usar TODAS las columnas del archivo Drapify
    PASO 2: Agregar Logistic de Anicam con lógica de fallback
    PASO 3: Agregar Aditionals de Anicam 
    PASO 4: Crear columna "Asignacion"
    PASO 5: Agregar datos de CXP
    """
    
    st.subheader("🔄 Procesando archivos según especificaciones del prompt")

    # === PASO 1: Base del Consolidado (Drapify) ===
    st.write("**PASO 1:** Cargando archivo base Drapify...")
    
    if not drapify_file:
        st.error("❌ El archivo DRAPIFY es requerido como base del consolidado.")
        return pd.DataFrame()
    
    df_drapify = leer_excel_o_csv(drapify_file)
    if df_drapify is None:
        st.error("❌ No se pudo leer el archivo DRAPIFY.")
        return pd.DataFrame()
    
    df_drapify = limpiar_nombres_columnas(df_drapify)
    
    # Convertir columnas numéricas conocidas
    numeric_cols = ['quantity', 'unit_price', 'declare_value', 'meli_fee', 'iva', 'ica', 
                   'senders_cost', 'gross_amount', 'net_received_amount', 'net_real_amount', 
                   'logistic_weight_lbs']
    
    for col in numeric_cols:
        if col in df_drapify.columns:
            df_drapify[col] = pd.to_numeric(df_drapify[col], errors='coerce').fillna(0)
    
    # Asegurar que las columnas de texto sean string y estén limpias
    text_cols = ['system_hash', 'serial_hash', 'order_id', 'account_name', 'prealert_id']
    for col in text_cols:
        if col in df_drapify.columns:
            df_drapify[col] = df_drapify[col].astype(str).str.strip()
    
    st.success(f"✅ Archivo DRAPIFY cargado: {len(df_drapify)} registros, {len(df_drapify.columns)} columnas")
    
    # Inicializar el DataFrame consolidado con Drapify
    df_consolidated = df_drapify.copy()

    # === PASO 2: Agregar Logistic de Anicam con lógica de fallback ===
    st.write("**PASO 2:** Agregando datos de Logistic de Anicam...")
    
    if logistic_file:
        df_logistic = leer_excel_o_csv(logistic_file)
        if df_logistic is not None:
            df_logistic = limpiar_nombres_columnas(df_logistic)
            
            # Limpiar columnas de unión
            if 'reference' in df_logistic.columns:
                df_logistic['reference'] = df_logistic['reference'].astype(str).str.strip()
            if 'order_number' in df_logistic.columns:
                df_logistic['order_number'] = df_logistic['order_number'].astype(str).str.strip()
            
            # Convertir columnas numéricas de logistic
            logistic_numeric_cols = ['fob', 'weight', 'length', 'width', 'height', 'insurance', 
                                   'logistics', 'duties_prealert', 'duties_pay', 'duty_fee', 
                                   'saving', 'total']
            for col in logistic_numeric_cols:
                if col in df_logistic.columns:
                    df_logistic[col] = pd.to_numeric(df_logistic[col], errors='coerce').fillna(0)
            
            # LÓGICA DE FALLBACK según el prompt:
            # 1. Buscar order_id (Drapify) en Reference (Logistic)
            # 2. Si no se encuentra, buscar prealert_id (Drapify) en Order number (Logistic)
            
            matches_found = 0
            fallback_matches_found = 0
            
            # Primera búsqueda: order_id → Reference
            if 'order_id' in df_consolidated.columns and 'reference' in df_logistic.columns:
                df_temp = pd.merge(df_consolidated, df_logistic,
                                 left_on='order_id', right_on='reference',
                                 how='left', indicator=True, suffixes=('', '_logistic'))
                
                matches_found = (df_temp['_merge'] == 'both').sum()
                
                # Identificar registros sin coincidencia para aplicar fallback
                unmatched_mask = df_temp['_merge'] == 'left_only'
                unmatched_indices = df_temp[unmatched_mask].index
                
                # Segunda búsqueda (fallback): prealert_id → Order number
                if 'prealert_id' in df_consolidated.columns and 'order_number' in df_logistic.columns and len(unmatched_indices) > 0:
                    st.info(f"🔄 Aplicando lógica de fallback para {len(unmatched_indices)} registros no encontrados...")
                    
                    # Para los registros no encontrados, intentar con prealert_id
                    df_unmatched = df_consolidated.loc[unmatched_indices].copy()
                    df_fallback = pd.merge(df_unmatched, df_logistic,
                                         left_on='prealert_id', right_on='order_number',
                                         how='left', suffixes=('', '_logistic'))
                    
                    fallback_matches_found = df_fallback['reference'].notna().sum()
                    
                    # Combinar resultados: usar coincidencias principales + fallback
                    df_matched = df_temp[df_temp['_merge'] == 'both'].drop(columns=['_merge'])
                    df_unmatched_original = df_temp[df_temp['_merge'] == 'left_only'].drop(columns=['_merge'])
                    
                    # Reemplazar datos de unmatched original con fallback donde sea posible
                    for idx in df_fallback.index:
                        original_idx = unmatched_indices[idx]
                        if pd.notna(df_fallback.loc[idx, 'reference']):  # Si el fallback encontró coincidencia
                            # Reemplazar la fila en df_unmatched_original con los datos del fallback
                            for col in df_logistic.columns:
                                col_name = col if col not in df_consolidated.columns else f"{col}_logistic"
                                if col_name in df_fallback.columns:
                                    df_unmatched_original.loc[original_idx, col_name] = df_fallback.loc[idx, col_name]
                    
                    df_consolidated = pd.concat([df_matched, df_unmatched_original], ignore_index=True)
                else:
                    df_consolidated = df_temp.drop(columns=['_merge'])
                
                st.success(f"✅ Logistic unido: {matches_found} coincidencias directas, {fallback_matches_found} por fallback")
            else:
                st.warning("⚠️ No se encontraron las columnas necesarias para unir con Logistic")
        else:
            st.warning("❌ No se pudo cargar el archivo Logistic")
    else:
        st.info("ℹ️ Archivo Logistic no proporcionado")

    # === PASO 3: Agregar Aditionals de Anicam ===
    st.write("**PASO 3:** Agregando datos de Aditionals de Anicam...")
    
    if aditionals_file:
        df_aditionals = leer_excel_o_csv(aditionals_file)
        if df_aditionals is not None:
            df_aditionals = limpiar_nombres_columnas(df_aditionals)
            
            # Limpiar columna de unión
            if 'order_id' in df_aditionals.columns:
                df_aditionals['order_id'] = df_aditionals['order_id'].astype(str).str.strip()
                # Renombrar para evitar conflictos
                df_aditionals = df_aditionals.rename(columns={'order_id': 'order_id_aditionals'})
            
            # Convertir columnas numéricas
            aditionals_numeric_cols = ['quantity', 'unitprice', 'total']
            for col in aditionals_numeric_cols:
                if col in df_aditionals.columns:
                    df_aditionals[col] = pd.to_numeric(df_aditionals[col], errors='coerce').fillna(0)
            
            # Unir usando prealert_id (Drapify) → Order Id (Aditionals)
            if 'prealert_id' in df_consolidated.columns and 'order_id_aditionals' in df_aditionals.columns:
                df_consolidated = pd.merge(df_consolidated, df_aditionals,
                                         left_on='prealert_id', right_on='order_id_aditionals',
                                         how='left', suffixes=('', '_adit'))
                
                aditionals_matches = df_consolidated['order_id_aditionals'].notna().sum()
                st.success(f"✅ Aditionals unido: {aditionals_matches} coincidencias encontradas")
            else:
                st.warning("⚠️ No se encontraron las columnas necesarias para unir con Aditionals")
        else:
            st.warning("❌ No se pudo cargar el archivo Aditionals")
    else:
        st.info("ℹ️ Archivo Aditionals no proporcionado")

    # === PASO 4: Crear columna "Asignacion" ===
    st.write("**PASO 4:** Creando columna 'Asignacion'...")
    
    if 'account_name' in df_consolidated.columns and 'serial_hash' in df_consolidated.columns:
        df_consolidated['asignacion'] = None
        
        # Aplicar la lógica según el prompt
        conditions = [
            (df_consolidated['account_name'] == "1-TODOENCARGO-CO", "TDC"),
            (df_consolidated['account_name'] == "2-MEGATIENDA SPA", "MEGA"),
            (df_consolidated['account_name'] == "4-MEGA TIENDAS PERUANAS", "MGA-PE"),
            (df_consolidated['account_name'] == "5-DETODOPARATODOS", "DTPT"),
            (df_consolidated['account_name'] == "6-COMPRAFACIL", "CFA"),
            (df_consolidated['account_name'] == "7-COMPRA-YA", "CPYA"),
            (df_consolidated['account_name'] == "8-FABORCARGO", "FBC"),
            (df_consolidated['account_name'] == "3-VEENDELO", "VEEN")
        ]
        
        for condition, prefix in conditions:
            df_consolidated.loc[condition, 'asignacion'] = prefix + df_consolidated.loc[condition, 'serial_hash']
        
        asignacion_count = df_consolidated['asignacion'].notna().sum()
        st.success(f"✅ Columna 'Asignacion' creada: {asignacion_count} valores asignados")
    else:
        st.warning("⚠️ No se pudo crear la columna 'Asignacion'. Faltan 'account_name' o 'serial_hash'")

    # === PASO 5: Agregar datos de CXP ===
    st.write("**PASO 5:** Agregando datos de CXP...")
    
    if cxp_file:
        df_cxp = try_read_cxp(cxp_file)
        if df_cxp is not None:
            # Encontrar la columna Ref # (puede tener diferentes nombres después de la limpieza)
            ref_col = None
            for col in df_cxp.columns:
                if 'ref' in col and ('hash' in col or 'ref_' in col):
                    ref_col = col
                    break
            
            if ref_col is None:
                # Buscar alternativas
                for col in df_cxp.columns:
                    if 'ref' in col:
                        ref_col = col
                        break
            
            if ref_col and 'asignacion' in df_consolidated.columns:
                # Limpiar columna de unión
                df_cxp[ref_col] = df_cxp[ref_col].astype(str).str.strip()
                df_consolidated['asignacion'] = df_consolidated['asignacion'].astype(str).str.strip()
                
                # Convertir columnas numéricas de CXP
                cxp_numeric_cols = ['co_aereo', 'arancel', 'iva', 'handling', 'dest_delivery', 'amt_due', 'goods_value']
                for col in cxp_numeric_cols:
                    # Buscar la columna con nombre similar
                    actual_col = None
                    for cxp_col in df_cxp.columns:
                        if col.replace('_', '') in cxp_col.replace('_', ''):
                            actual_col = cxp_col
                            break
                    
                    if actual_col:
                        df_cxp[actual_col] = pd.to_numeric(df_cxp[actual_col], errors='coerce').fillna(0)
                
                # Unir usando Asignacion → Ref #
                df_consolidated = pd.merge(df_consolidated, df_cxp,
                                         left_on='asignacion', right_on=ref_col,
                                         how='left', suffixes=('', '_cxp'))
                
                cxp_matches = df_consolidated[ref_col].notna().sum()
                st.success(f"✅ CXP unido: {cxp_matches} coincidencias encontradas")
            else:
                st.warning("⚠️ No se encontraron las columnas necesarias para unir con CXP")
        else:
            st.warning("❌ No se pudo cargar el archivo CXP")
    else:
        st.info("ℹ️ Archivo CXP no proporcionado")

    # Limpiar datos finales
    df_consolidated = df_consolidated.replace({np.nan: None, pd.NaT: None, '': None})
    
    st.success(f"🎉 ¡Consolidación completada! Total: {len(df_consolidated)} registros, {len(df_consolidated.columns)} columnas")
    
    return df_consolidated

def save_to_supabase(df_to_save):
    """Guarda el DataFrame consolidado en Supabase."""
    if df_to_save.empty:
        st.warning("No hay datos para guardar en Supabase.")
        return

    st.subheader("💾 Guardando datos en Supabase...")

    # Añadir timestamp de procesamiento
    df_to_save['processed_at_app'] = datetime.now().isoformat()

    # Convertir DataFrame a lista de diccionarios
    records_to_upload = []
    for _, row in df_to_save.iterrows():
        record = {}
        for col in df_to_save.columns:
            value = row[col]
            # Convertir valores nulos y tipos especiales
            if pd.isna(value) or pd.isnull(value):
                record[col] = None
            elif isinstance(value, (np.integer, int)):
                record[col] = int(value)
            elif isinstance(value, (np.floating, float)):
                record[col] = float(value) if not np.isnan(value) else None
            else:
                record[col] = str(value) if value is not None else None
        
        records_to_upload.append(record)

    # Eliminar duplicados basados en order_id si existe
    if 'order_id' in df_to_save.columns:
        seen_ids = set()
        unique_records = []
        duplicates_count = 0
        
        for record in records_to_upload:
            order_id = record.get('order_id')
            if order_id and order_id not in seen_ids:
                seen_ids.add(order_id)
                unique_records.append(record)
            elif order_id:
                duplicates_count += 1
            else:
                unique_records.append(record)  # Incluir registros sin order_id
        
        records_to_upload = unique_records
        
        if duplicates_count > 0:
            st.warning(f"⚠️ Se omitieron {duplicates_count} registros duplicados")

    st.info(f"📊 Preparando para insertar {len(records_to_upload)} registros...")

    try:
        # Insertar en Supabase en lotes para evitar timeouts
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(records_to_upload), batch_size):
            batch = records_to_upload[i:i+batch_size]
            
            response = supabase.table('orders').insert(batch).execute()
            
            if response.data:
                total_inserted += len(response.data)
                st.progress((i + len(batch)) / len(records_to_upload))
        
        st.success(f"💾 ¡Datos guardados exitosamente! Total insertado: {total_inserted} registros")
        
    except Exception as e:
        st.error(f"❌ Error al guardar en Supabase: {e}")
        st.exception(e)

# --- Páginas de la Aplicación ---
def page_process_files():
    st.markdown("<h1>📂 Consolidación de Archivos según Prompt</h1>", unsafe_allow_html=True)
    st.info("Sube tus archivos siguiendo el orden especificado en el prompt. El sistema procesará exactamente como se indica.")

    # Mostrar el orden de procesamiento según el prompt
    st.markdown("""
    ### 📋 Orden de Procesamiento (según prompt):
    1. **📄 DRAPIFY** (Base) - Todas las columnas como estructura base
    2. **🚚 Logistic Anicam** - Unir con fallback: order_id→Reference, luego prealert_id→Order number  
    3. **➕ Aditionals Anicam** - Unir: prealert_id→Order Id
    4. **🧮 Calcular Asignacion** - Basado en account_name + Serial#
    5. **🇨🇱 CXP ChileExpress** - Unir: Asignacion→Ref #
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Archivos Principales")
        drapify_file = st.file_uploader("📄 **DRAPIFY** (Requerido)", type=["csv", "xlsx", "xls"], key="drapify")
        logistic_file = st.file_uploader("🚚 **Logistic Anicam**", type=["csv", "xlsx", "xls"], key="logistic")

    with col2:
        st.subheader("Archivos Complementarios")
        aditionals_file = st.file_uploader("➕ **Aditionals Anicam**", type=["csv", "xlsx", "xls"], key="aditionals")
        cxp_file = st.file_uploader("🇨🇱 **CXP ChileExpress**", type=["csv", "xlsx", "xls"], key="cxp")

    st.markdown("---")
    
    if st.button("🚀 **Procesar Archivos según Prompt**", type="primary"):
        if drapify_file:
            with st.spinner("Procesando archivos según especificaciones del prompt..."):
                df_consolidated = process_files_according_to_prompt(
                    drapify_file, logistic_file, aditionals_file, cxp_file
                )
                
                if not df_consolidated.empty:
                    st.session_state['df_consolidated'] = df_consolidated
                    
                    # Mostrar estadísticas de procesamiento
                    st.subheader("📊 Estadísticas de Consolidación")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Registros", len(df_consolidated))
                    with col2:
                        logistic_matches = df_consolidated.get('reference', pd.Series()).notna().sum()
                        st.metric("Con Logistic", logistic_matches)
                    with col3:
                        aditionals_matches = df_consolidated.get('order_id_aditionals', pd.Series()).notna().sum()
                        st.metric("Con Aditionals", aditionals_matches)
                    with col4:
                        cxp_matches = 0
                        for col in df_consolidated.columns:
                            if 'ref' in col and 'cxp' in col:
                                cxp_matches = df_consolidated[col].notna().sum()
                                break
                        st.metric("Con CXP", cxp_matches)
                    
                    # Vista previa de datos
                    st.subheader("👀 Vista Previa del Consolidado")
                    st.dataframe(df_consolidated.head(10), use_container_width=True)
                    
                    # Opción para guardar en Supabase
                    if st.button("💾 **Guardar en Supabase**", type="secondary"):
                        save_to_supabase(df_consolidated)
                else:
                    st.error("⚠️ No se pudieron procesar los archivos. Revisa los mensajes anteriores.")
        else:
            st.warning("Por favor, carga al menos el archivo **DRAPIFY** para iniciar el procesamiento.")

def page_view_data():
    st.markdown("<h1>📊 Ver Datos Consolidados</h1>", unsafe_allow_html=True)
    st.info("Aquí puedes ver los datos que han sido cargados y consolidados en la base de datos.")

    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 **Recargar Datos**"):
            st.cache_data.clear()
            st.rerun()
    
    with col2:
        limit = st.selectbox("Límite de registros", [100, 500, 1000], index=1)

    try:
        response = supabase.table('orders').select('*').limit(limit).order('created_at', desc=True).execute()
        
        if response.data:
            df_db = pd.DataFrame(response.data)
            st.success(f"✅ Se cargaron {len(df_db)} registros de la base de datos.")
            
            # Mostrar estadísticas básicas
            st.subheader("📈 Estadísticas Básicas")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Registros", len(df_db))
            with col2:
                if 'account_name' in df_db.columns:
                    unique_accounts = df_db['account_name'].nunique()
                    st.metric("Cuentas Únicas", unique_accounts)
            with col3:
                if 'processed_at_app' in df_db.columns:
                    latest_process = df_db['processed_at_app'].max()
                    st.metric("Último Proceso", latest_process[:10] if latest_process else "N/A")
            
            # Filtros
            st.subheader("🔍 Filtros")
            filter_col1, filter_col2 = st.columns(2)
            
            with filter_col1:
                if 'account_name' in df_db.columns:
                    accounts = ['Todos'] + list(df_db['account_name'].dropna().unique())
                    selected_account = st.selectbox("Filtrar por cuenta", accounts)
                    
                    if selected_account != 'Todos':
                        df_db = df_db[df_db['account_name'] == selected_account]
            
            with filter_col2:
                if 'order_status_meli' in df_db.columns:
                    statuses = ['Todos'] + list(df_db['order_status_meli'].dropna().unique())
                    selected_status = st.selectbox("Filtrar por estado", statuses)
                    
                    if selected_status != 'Todos':
                        df_db = df_db[df_db['order_status_meli'] == selected_status]
            
            # Mostrar datos filtrados
            st.subheader("📋 Datos")
            st.dataframe(df_db, use_container_width=True)
            
            # Opción para descargar
            if not df_db.empty:
                csv = df_db.to_csv(index=False)
                st.download_button(
                    label="📥 Descargar CSV",
                    data=csv,
                    file_name=f"datos_consolidados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
        else:
            st.info("ℹ️ No hay datos en la tabla 'orders' aún. ¡Procesa algunos archivos primero!")
            
    except Exception as e:
        st.error(f"❌ Error al cargar datos: {e}")
        st.exception(e)

def page_debug_schema():
    st.markdown("<h1>🔧 Verificación de Sistema</h1>", unsafe_allow_html=True)
    st.info("Utiliza esta sección para verificar la configuración y probar la conexión.")
    
    # Test de conexión
    st.subheader("🔌 Estado de Conexión")
    try:
        test_response = supabase.table('orders').select('id').limit(1).execute()
        st.success("✅ Conexión a Supabase **OK**")
        
        # Estadísticas de la tabla
        count_response = supabase.table('orders').select('id', count='exact').execute()
        total_records = count_response.count if count_response.count is not None else 0
        st.info(f"📊 Total de registros en la tabla: **{total_records}**")
        
    except Exception as e:
        st.error(f"❌ Error de conexión: {e}")
    
    st.markdown("---")
    
    # Verificar estructura de tabla
    st.subheader("📋 Estructura de Tabla")
    if st.button("🔍 **Verificar Columnas de Supabase**"):
        valid_columns = get_valid_supabase_columns()
        if valid_columns:
            st.success(f"✅ Encontradas {len(valid_columns)} columnas en la tabla 'orders'")
            
            # Mostrar columnas en categorías
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Columnas Base (Drapify):**")
                base_cols = [col for col in valid_columns if not any(x in col for x in ['logistic', 'aditional', 'cxp', 'asignacion'])]
                for col in sorted(base_cols)[:20]:  # Mostrar primeras 20
                    st.write(f"• `{col}`")
                if len(base_cols) > 20:
                    st.write(f"... y {len(base_cols)-20} más")
            
            with col2:
                st.write("**Columnas Adicionales:**")
                additional_cols = [col for col in valid_columns if any(x in col for x in ['logistic', 'aditional', 'cxp', 'asignacion'])]
                for col in sorted(additional_cols):
                    st.write(f"• `{col}`")
        else:
            st.error("❌ No se pudieron obtener las columnas de la tabla")
            
            # Sugerir creación de tabla
            st.warning("La tabla 'orders' podría no existir o estar vacía.")
            
            if st.button("📝 **Mostrar SQL para crear tabla**"):
                st.code("""
-- SQL para crear la tabla orders básica
CREATE TABLE public.orders (
    id bigint GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    
    -- Columnas base de Drapify
    system_hash text,
    serial_hash text,
    order_id text,
    pack_id text,
    asin text,
    client_first_name text,
    client_last_name text,
    client_doc_id text,
    account_name text,
    date_created text,
    quantity numeric,
    title text,
    unit_price numeric,
    logistic_type text,
    address_line text,
    street_name text,
    street_number text,
    city text,
    state text,
    country text,
    receiver_phone text,
    amz_order_id text,
    prealert_id text,
    etiqueta_envio text,
    order_status_meli text,
    declare_value numeric,
    meli_fee numeric,
    iva numeric,
    ica numeric,
    fuente text,
    senders_cost numeric,
    gross_amount numeric,
    net_received_amount numeric,
    nombre_del_tercero text,
    direccion text,
    apelido_del_tercero text,
    estado text,
    razon_social text,
    ciudad text,
    numero_de_documento text,
    digital_verification text,
    tipo text,
    telefono text,
    giro text,
    correo text,
    net_real_amount numeric,
    logistic_weight_lbs numeric,
    refunded_date text,
    
    -- Columnas de Logistic
    guide_number text,
    order_number text,
    reference text,
    sap_code text,
    invoice_logistic text,
    status text,
    fob numeric,
    unit text,
    weight numeric,
    length numeric,
    width numeric,
    height numeric,
    insurance numeric,
    logistics numeric,
    duties_prealert numeric,
    duties_pay numeric,
    duty_fee numeric,
    saving numeric,
    total numeric,
    description text,
    shipper text,
    phone_shipper text,
    consignee text,
    identification text,
    country_logistic text,
    state_logistic text,
    city_logistic text,
    address_logistic text,
    phone_consignee text,
    master_guide text,
    tariff_position text,
    external_id text,
    invoice_anicam text,
    
    -- Columnas de Aditionals
    order_id_aditionals text,
    item text,
    reference_aditionals text,
    description_aditionals text,
    quantity_aditionals numeric,
    unitprice numeric,
    total_aditionals numeric,
    
    -- Columna calculada
    asignacion text,
    
    -- Columnas de CXP
    ot_number text,
    date_cxp text,
    ref_hash text,
    consignee_cxp text,
    co_aereo numeric,
    arancel numeric,
    iva_cxp numeric,
    handling numeric,
    dest_delivery numeric,
    amt_due numeric,
    goods_value numeric,
    
    -- Metadatos
    processed_at_app timestamp with time zone,
    
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);

-- Habilitar Row Level Security si es necesario
ALTER TABLE public.orders ENABLE ROW LEVEL SECURITY;
                """, language="sql")
    
    st.markdown("---")
    
    # Información del sistema
    st.subheader("ℹ️ Información del Sistema")
    
    info_col1, info_col2 = st.columns(2)
    
    with info_col1:
        st.write("**Formatos Soportados:**")
        st.write("• CSV (.csv)")
        st.write("• Excel (.xlsx, .xls)")
        
        st.write("**Encodings CSV:**")
        st.write("• UTF-8")
        st.write("• Latin-1")
        st.write("• CP1252")
        st.write("• ISO-8859-1")
    
    with info_col2:
        st.write("**Proceso de Unión:**")
        st.write("1. Drapify (base)")
        st.write("2. Logistic (con fallback)")
        st.write("3. Aditionals")
        st.write("4. Calcular Asignacion")
        st.write("5. CXP")

# --- Lógica principal de la Aplicación ---
st.sidebar.title("🏢 Sistema Contable Multi-País")
st.sidebar.markdown("**Versión Corregida según Prompt**")
st.sidebar.markdown("---")

# Estado de conexión en sidebar
st.sidebar.markdown("### 📊 Estado del Sistema")
try:
    test_connection = supabase.table('orders').select('id').limit(1).execute()
    st.sidebar.success("🟢 Supabase Conectado")
except:
    st.sidebar.error("🔴 Error de Conexión")

# Información del proceso
st.sidebar.markdown("### 🔄 Proceso de Consolidación")
st.sidebar.markdown("""
**Según Prompt:**
1. **Base:** Drapify (todas las columnas)
2. **+Logistic:** order_id→Reference (fallback: prealert_id→Order number)
3. **+Aditionals:** prealert_id→Order Id
4. **+Asignacion:** account_name + Serial#
5. **+CXP:** Asignacion→Ref #
""")

st.sidebar.markdown("---")

# Navegación
page_selection = st.sidebar.radio("🧭 **Navegación**", [
    "📂 Procesar Archivos",
    "📊 Ver Datos", 
    "🔧 Verificar Sistema"
])

# Renderizar página seleccionada
if page_selection == "📂 Procesar Archivos":
    page_process_files()
elif page_selection == "📊 Ver Datos":
    page_view_data()
elif page_selection == "🔧 Verificar Sistema":
    page_debug_schema()

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("### 📝 Notas")
st.sidebar.info("Esta versión implementa exactamente las especificaciones del prompt, incluyendo la lógica de fallback para Logistic.")

# Información de sessión si hay datos procesados
if 'df_consolidated' in st.session_state:
    st.sidebar.markdown("### 💾 Sesión Actual")
    df_session = st.session_state['df_consolidated']
    st.sidebar.success(f"✅ {len(df_session)} registros procesados")
    
    if st.sidebar.button("🗑️ Limpiar Sesión"):
        del st.session_state['df_consolidated']
        st.rerun()
