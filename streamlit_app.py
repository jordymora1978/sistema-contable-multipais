import streamlit as st
import pandas as pd
import numpy as np
from supabase import create_client, Client
import os
from datetime import datetime
import io
import time
import re
import gc  # Garbage collector para liberación de memoria

# Configuración optimizada para archivos grandes
st.set_page_config(
    page_title="Consolidador de Órdenes - Optimizado",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CONFIGURACIONES PARA ARCHIVOS GRANDES
CHUNK_SIZE = 500  # Tamaño de chunk para procesamiento
BATCH_SIZE = 25   # Tamaño de batch para inserción en BD (reducido)

# Configuración de Supabase
@st.cache_resource
def init_supabase():
    url = "https://pvbzzpeyhhxexyabizbv.supabase.co"
    key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB2Ynp6cGV5aGh4ZXh5YWJpemJ2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM5OTc5ODcsImV4cCI6MjA2OTU3Mzk4N30.06S8jDjNReAd6Oj8AZvOS2PUcO2ASJHVA3VUNYVeAR4"
    return create_client(url, key)

supabase = init_supabase()

def clean_memory():
    """Libera memoria forzando garbage collection"""
    gc.collect()
    time.sleep(0.1)  # Pequeña pausa para permitir limpieza

def read_file_optimized(file_obj, file_type="unknown"):
    """Lee archivos con optimización para archivos grandes"""
    st.info(f"📖 Leyendo archivo {file_type}...")
    
    try:
        file_size = file_obj.size / (1024 * 1024)  # Tamaño en MB
        st.info(f"📊 Tamaño del archivo: {file_size:.2f} MB")
        
        if file_obj.name.endswith('.csv'):
            # Para CSV grandes, usar chunks
            if file_size > 50:  # Si es mayor a 50MB
                st.info("🔄 Archivo grande detectado. Leyendo en chunks...")
                chunks = []
                
                # Leer en chunks para archivos grandes
                try:
                    chunk_reader = pd.read_csv(file_obj, chunksize=CHUNK_SIZE)
                    chunk_count = 0
                    
                    progress_bar = st.progress(0)
                    
                    for chunk in chunk_reader:
                        chunks.append(chunk)
                        chunk_count += 1
                        
                        # Mostrar progreso cada 10 chunks
                        if chunk_count % 10 == 0:
                            st.info(f"📋 Procesando chunk {chunk_count}...")
                            clean_memory()
                            progress_bar.progress(min(1.0, chunk_count * CHUNK_SIZE / (file_size * 100)))
                    
                    progress_bar.progress(1.0)
                    df = pd.concat(chunks, ignore_index=True)
                    del chunks  # Liberar memoria
                    clean_memory()
                    
                except Exception as chunk_error:
                    st.warning(f"⚠️ Error leyendo en chunks: {chunk_error}")
                    st.info("🔄 Intentando lectura normal...")
                    file_obj.seek(0)  # Resetear archivo
                    df = pd.read_csv(file_obj)
            else:
                # Archivo pequeño, leer normalmente
                df = pd.read_csv(file_obj)
        else:
            # Para Excel
            if file_size > 20:  # Si es mayor a 20MB
                st.info("🔄 Archivo Excel grande. Usando engine optimizado...")
                df = pd.read_excel(file_obj, engine='openpyxl')
            else:
                df = pd.read_excel(file_obj)
        
        st.success(f"✅ {file_type} cargado: {len(df):,} registros, {len(df.columns)} columnas")
        
        # Optimizar memoria del DataFrame
        df = optimize_dataframe_memory(df)
        
        return df
        
    except MemoryError:
        st.error(f"❌ Error de memoria al leer {file_type}. Archivo demasiado grande.")
        return None
    except Exception as e:
        st.error(f"❌ Error leyendo {file_type}: {str(e)}")
        return None

def optimize_dataframe_memory(df):
    """Optimiza el uso de memoria del DataFrame"""
    if df is None or df.empty:
        return df
    
    st.info("🎯 Optimizando uso de memoria...")
    
    try:
        initial_memory = df.memory_usage(deep=True).sum() / 1024**2
        
        # Optimizar tipos de datos
        for col in df.columns:
            col_type = df[col].dtype
            
            # Convertir object a category si tiene pocos valores únicos
            if col_type == 'object':
                try:
                    unique_ratio = df[col].nunique() / len(df)
                    if unique_ratio < 0.5:  # Si menos del 50% son únicos
                        df[col] = df[col].astype('category')
                except:
                    pass  # Si falla, continuar
            
            # Optimizar enteros
            elif col_type in ['int64']:
                try:
                    df[col] = pd.to_numeric(df[col], downcast='integer')
                except:
                    pass
            
            # Optimizar flotantes
            elif col_type in ['float64']:
                try:
                    df[col] = pd.to_numeric(df[col], downcast='float')
                except:
                    pass
        
        final_memory = df.memory_usage(deep=True).sum() / 1024**2
        reduction = ((initial_memory - final_memory) / initial_memory * 100) if initial_memory > 0 else 0
        
        st.success(f"✅ Memoria optimizada: {initial_memory:.1f}MB → {final_memory:.1f}MB ({reduction:.1f}% reducción)")
        
    except Exception as e:
        st.warning(f"⚠️ Error optimizando memoria: {str(e)}")
    
    return df

def clean_id_optimized(value):
    """Versión optimizada de clean_id"""
    if pd.isna(value) or value is None:
        return None
    
    try:
        str_value = str(value).strip()
        if str_value.lower() in ['nan', 'none', 'null', '']:
            return None
        
        str_value = str_value.strip("'\"")
        if str_value.endswith('.0') and str_value[:-2].isdigit():
            str_value = str_value[:-2]
        
        return str_value if str_value else None
    except:
        return None

def calculate_asignacion(account_name, serial_number):
    """Calcula la asignación basada en el account_name y serial_number"""
    if pd.isna(account_name) or pd.isna(serial_number):
        return None
    
    clean_serial = clean_id_optimized(serial_number)
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

def process_matching_optimized(base_df, secondary_df, match_rules, prefix):
    """Procesa matching entre DataFrames optimizado para archivos grandes"""
    if secondary_df is None or secondary_df.empty:
        return base_df
    
    st.info(f"🔗 Procesando matching {prefix}...")
    
    # Crear índices para matching rápido
    match_dicts = {}
    for rule in match_rules:
        base_col, secondary_col = rule
        if secondary_col in secondary_df.columns:
            match_dict = {}
            for idx, row in secondary_df.iterrows():
                key = clean_id_optimized(row.get(secondary_col, ''))
                if key:
                    match_dict[key] = row
            match_dicts[base_col] = (secondary_col, match_dict)
            st.info(f"📋 Índice creado para {base_col} -> {secondary_col}: {len(match_dict)} entradas")
    
    # Agregar columnas del archivo secundario
    for col in secondary_df.columns:
        new_col_name = f'{prefix}_{col.lower().replace(" ", "_").replace(".", "").replace("#", "number")}'
        base_df[new_col_name] = np.nan
    
    # Procesar en chunks para evitar problemas de memoria
    chunk_size = min(CHUNK_SIZE, len(base_df))
    total_chunks = (len(base_df) + chunk_size - 1) // chunk_size
    matched_count = 0
    
    progress_bar = st.progress(0)
    
    for chunk_idx in range(total_chunks):
        start_idx = chunk_idx * chunk_size
        end_idx = min(start_idx + chunk_size, len(base_df))
        
        # Procesar chunk
        for idx in range(start_idx, end_idx):
            row = base_df.iloc[idx]
            matched_row = None
            
            # Probar cada regla de matching
            for base_col, (secondary_col, match_dict) in match_dicts.items():
                key = clean_id_optimized(row.get(base_col, ''))
                if key and key in match_dict:
                    matched_row = match_dict[key]
                    matched_count += 1
                    break
            
            # Si encontró match, copiar datos
            if matched_row is not None:
                for col in secondary_df.columns:
                    new_col_name = f'{prefix}_{col.lower().replace(" ", "_").replace(".", "").replace("#", "number")}'
                    base_df.loc[idx, new_col_name] = matched_row.get(col)
        
        # Actualizar progreso
        progress = (chunk_idx + 1) / total_chunks
        progress_bar.progress(progress)
        
        # Limpiar memoria cada 20 chunks
        if chunk_idx % 20 == 0:
            clean_memory()
    
    st.success(f"✅ {prefix} procesado: {matched_count:,} matches encontrados")
    return base_df

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
        'estado': 'estado',
        'razon_social': 'razon_social',
        'ciudad': 'ciudad',
        'numero_de_documento': 'numero_de_documento',
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
    """Aplicar formatos básicos sin afectar campos numéricos para BD"""
    if df is None or df.empty:
        return df
    
    st.info("🔧 Aplicando formatos básicos...")
    
    # Corrección de encoding en columnas de texto
    text_columns = [
        'client_first_name', 'client_last_name', 'title', 'address_line', 
        'street_name', 'city', 'state', 'country', 'nombre_del_tercero',
        'direccion', 'apelido_del_tercero', 'estado', 'razon_social', 'ciudad'
    ]
    
    for col in text_columns:
        if col in df.columns:
            try:
                df[col] = df[col].astype(str).str.strip()
            except:
                pass
    
    st.success("✅ Formatos básicos aplicados")
    return df

def insert_to_supabase_optimized(df):
    """Inserción optimizada en Supabase para archivos grandes"""
    try:
        st.info("🔍 Preparando datos para inserción optimizada...")
        
        # Mapear columnas
        df_mapped = map_column_names(df)
        
        # Columnas válidas para la BD
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
        
        # Agregar columnas dinámicas
        for col in df_mapped.columns:
            if (col.startswith('logistics_') or col.startswith('aditionals_') or col.startswith('cxp_')) and col not in db_columns:
                db_columns.append(col)
        
        # Filtrar DataFrame
        df_filtered = df_mapped[[col for col in db_columns if col in df_mapped.columns]]
        
        st.info(f"📊 Preparando {len(df_filtered):,} registros con {len(df_filtered.columns)} columnas")
        
        # Procesar en batches pequeños
        total_records = len(df_filtered)
        batch_size = BATCH_SIZE
        total_batches = (total_records + batch_size - 1) // batch_size
        
        total_inserted = 0
        errors = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Procesar en batches
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, total_records)
            
            batch_df = df_filtered.iloc[start_idx:end_idx]
            
            # Convertir a registros y limpiar
            batch_records = []
            for _, row in batch_df.iterrows():
                record = {}
                for col, value in row.items():
                    if pd.isna(value):
                        record[col] = None
                    elif isinstance(value, (np.integer, np.floating)):
                        if np.isfinite(value):
                            record[col] = float(value) if isinstance(value, np.floating) else int(value)
                        else:
                            record[col] = None
                    else:
                        record[col] = value
                batch_records.append(record)
            
            # Insertar batch
            try:
                result = supabase.table('consolidated_orders').insert(batch_records).execute()
                total_inserted += len(batch_records)
                
                # Actualizar progreso
                progress = (batch_idx + 1) / total_batches
                progress_bar.progress(progress)
                status_text.text(f"Insertando: {total_inserted:,}/{total_records:,} registros (Batch {batch_idx + 1}/{total_batches})")
                
                # Liberar memoria cada 20 batches
                if batch_idx % 20 == 0:
                    del batch_records
                    clean_memory()
                
            except Exception as batch_error:
                error_msg = f"Error en batch {batch_idx + 1}: {str(batch_error)}"
                st.error(error_msg)
                errors.append(error_msg)
                continue
        
        progress_bar.progress(1.0)
        status_text.text(f"✅ Completado: {total_inserted:,} registros insertados")
        
        return total_inserted
        
    except Exception as e:
        st.error(f"Error general en inserción: {str(e)}")
        return 0

def process_files_for_large_datasets(drapify_df, logistics_df=None, aditionals_df=None, cxp_df=None):
    """Procesamiento optimizado para datasets grandes"""
    
    st.info("🚀 Iniciando procesamiento optimizado para archivos grandes...")
    
    # PASO 1: Optimizar archivo base
    st.info("📋 Procesando archivo base Drapify...")
    consolidated_df = drapify_df.copy()
    clean_memory()
    
    # PASO 2: Procesar Logistics
    if logistics_df is not None:
        matching_rules = [
            ('order_id', 'Reference'),
            ('prealert_id', 'Order number')
        ]
        consolidated_df = process_matching_optimized(
            consolidated_df, logistics_df, matching_rules, 'logistics'
        )
        clean_memory()
    
    # PASO 3: Procesar Aditionals
    if aditionals_df is not None:
        matching_rules = [('prealert_id', 'Order Id')]
        consolidated_df = process_matching_optimized(
            consolidated_df, aditionals_df, matching_rules, 'aditionals'
        )
        clean_memory()
    
    # PASO 4: Calcular Asignación
    st.info("🏷️ Calculando asignaciones...")
    if 'account_name' in consolidated_df.columns and 'Serial#' in consolidated_df.columns:
        consolidated_df['Asignacion'] = consolidated_df.apply(
            lambda row: calculate_asignacion(row['account_name'], row['Serial#']), 
            axis=1
        )
    clean_memory()
    
    # PASO 5: Procesar CXP
    if cxp_df is not None:
        # Normalizar columnas CXP
        if 'Ref #' not in cxp_df.columns and 'ref_number' in cxp_df.columns:
            cxp_df = cxp_df.rename(columns={'ref_number': 'Ref #'})
        
        matching_rules = [('Asignacion', 'Ref #')]
        consolidated_df = process_matching_optimized(
            consolidated_df, cxp_df, matching_rules, 'cxp'
        )
        clean_memory()
    
    # PASO 6: Aplicar formatos básicos
    consolidated_df = apply_basic_formatting(consolidated_df)
    
    st.success(f"🎉 Procesamiento optimizado completado: {len(consolidated_df):,} registros")
    return consolidated_df

def main():
    """Función principal optimizada"""
    
    st.title("📦 Consolidador de Órdenes - Optimizado para Archivos Grandes")
    st.markdown("### 🚀 Procesamiento eficiente sin dependencias externas")
    
    # Test de conexión
    try:
        test_result = supabase.table('consolidated_orders').select('id').limit(1).execute()
        st.sidebar.success("✅ Conectado a Supabase")
    except Exception as e:
        st.sidebar.error(f"❌ Error de conexión: {str(e)}")
    
    # Configuración optimizada
    with st.sidebar:
        st.header("⚙️ Configuración Optimizada")
        
        st.subheader("🔧 Optimizaciones Activas")
        st.success("✅ Lectura en chunks para archivos >50MB")
        st.success("✅ Procesamiento en batches")
        st.success("✅ Gestión automática de memoria")
        st.success("✅ Sin dependencias externas")
        
        st.info(f"📊 Chunk size: {CHUNK_SIZE:,} registros")
        st.info(f"📦 Batch size BD: {BATCH_SIZE} registros")
    
    # Área de carga de archivos
    st.header("📁 Subir Archivos (Optimizado para Archivos Grandes)")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        drapify_file = st.file_uploader(
            "1. Archivo Drapify (OBLIGATORIO)",
            type=['xlsx', 'xls', 'csv'],
            key="drapify",
            help="✅ Optimizado para archivos grandes (>100MB)"
        )
        
        logistics_file = st.file_uploader(
            "2. Archivo Logistics (opcional)",
            type=['xlsx', 'xls', 'csv'],
            key="logistics"
        )
        
        aditionals_file = st.file_uploader(
            "3. Archivo Aditionals (opcional)",
            type=['xlsx', 'xls', 'csv'],
            key="aditionals"
        )
        
        cxp_file = st.file_uploader(
            "4. Archivo CXP (opcional)",
            type=['xlsx', 'xls', 'csv'],
            key="cxp"
        )
    
    with col2:
        st.header("📊 Estado")
        files_loaded = []
        if drapify_file:
            files_loaded.append(f"✅ Drapify ({drapify_file.size/1024/1024:.1f}MB)")
        if logistics_file:
            files_loaded.append(f"✅ Logistics ({logistics_file.size/1024/1024:.1f}MB)")
        if aditionals_file:
            files_loaded.append(f"✅ Aditionals ({aditionals_file.size/1024/1024:.1f}MB)")
        if cxp_file:
            files_loaded.append(f"✅ CXP ({cxp_file.size/1024/1024:.1f}MB)")
        
        for file_info in files_loaded:
            st.write(file_info)
    
    # Botón de procesamiento optimizado
    if st.button("🚀 Procesar con Optimización Avanzada", 
                 disabled=not drapify_file, type="primary"):
        
        with st.spinner("Procesando archivos grandes de forma optimizada..."):
            try:
                # Limpiar memoria antes de empezar
                clean_memory()
                
                # Leer archivos con optimización
                drapify_df = read_file_optimized(drapify_file, "Drapify")
                if drapify_df is None:
                    st.error("❌ Error crítico leyendo archivo Drapify")
                    st.stop()
                
                logistics_df = None
                if logistics_file:
                    logistics_df = read_file_optimized(logistics_file, "Logistics")
                
                aditionals_df = None
                if aditionals_file:
                    aditionals_df = read_file_optimized(aditionals_file, "Aditionals")
                
                cxp_df = None
                if cxp_file:
                    cxp_df = read_file_optimized(cxp_file, "CXP")
                
                # Procesar con optimización
                consolidated_df = process_files_for_large_datasets(
                    drapify_df, logistics_df, aditionals_df, cxp_df
                )
                
                # Mostrar preview limitado
                st.header("👀 Preview de Datos (Primeros 20 registros)")
                st.dataframe(consolidated_df.head(20), use_container_width=True)
                
                # Estadísticas
                st.header("📊 Estadísticas del Procesamiento")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Total Registros", f"{len(consolidated_df):,}")
                with col2:
                    st.metric("Columnas", len(consolidated_df.columns))
                with col3:
                    memory_usage = consolidated_df.memory_usage(deep=True).sum() / 1024**2
                    st.metric("Memoria DataFrame", f"{memory_usage:.1f}MB")
                
                # Guardar en BD con optimización
                st.header("💾 Guardando en Base de Datos (Optimizado)")
                
                with st.spinner("Insertando datos optimizados..."):
                    inserted_count = insert_to_supabase_optimized(consolidated_df)
                    
                    if inserted_count > 0:
                        st.success(f"🎉 ¡Archivos grandes procesados exitosamente!")
                        st.success(f"✅ {len(consolidated_df):,} registros procesados")
                        st.success(f"✅ {inserted_count:,} registros guardados en BD")
                        st.balloons()
                    else:
                        st.error("❌ Error guardando en BD")
                
                # Liberar memoria final
                del consolidated_df
                clean_memory()
                st.info("🧹 Memoria liberada exitosamente")
                
            except MemoryError:
                st.error("❌ Error de memoria. Los archivos son demasiado grandes.")
                st.info("💡 Sugerencia: Divide los archivos en partes más pequeñas.")
            except Exception as e:
                st.error(f"❌ Error procesando archivos: {str(e)}")
                st.exception(e)

if __name__ == "__main__":
    main()
