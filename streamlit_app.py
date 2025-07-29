import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
import time
import re

# ============================
# FUNCIONES DE NEGOCIO MEJORADAS
# ============================

def to_snake_case(name):
    """Función auxiliar para normalizar nombres de columnas a snake_case"""
    name = str(name)
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    name = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', name)
    return name.lower().replace(' ', '_').replace('.', '').replace('#', '').replace('-', '_')

def get_store_config_updated():
    """Configuración actualizada de tiendas con corrección de país para FABORCARGO"""
    try:
        supabase = init_supabase()
        if supabase:
            result = supabase.table('store_config').select('*').eq('activa', True).execute()
            if result.data:
                return {row['account_name']: {
                    'prefijo': row['prefijo'],
                    'pais': row['pais'],
                    'tipo_calculo': row['tipo_calculo']
                } for row in result.data}
        return {}
    except Exception as e:
        st.error(f"Error obteniendo configuración: {str(e)}")
        # Configuración de fallback con corrección de FABORCARGO
        return {
            '1-TODOENCARGO-CO': {'prefijo': 'TDC', 'pais': 'Colombia', 'tipo_calculo': 'A'},
            '2-MEGATIENDA SPA': {'prefijo': 'MEGA', 'pais': 'Chile', 'tipo_calculo': 'B'},
            '3-VEENDELO': {'prefijo': 'VEEN', 'pais': 'Colombia', 'tipo_calculo': 'B'},
            '4-MEGA TIENDAS PERUANAS': {'prefijo': 'MGA-PE', 'pais': 'Perú', 'tipo_calculo': 'A'},
            '5-DETODOPARATODOS': {'prefijo': 'DTPT', 'pais': 'Colombia', 'tipo_calculo': 'C'},
            '6-COMPRAFACIL': {'prefijo': 'CFA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
            '7-COMPRA-YA': {'prefijo': 'CPYA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
            '8-FABORCARGO': {'prefijo': 'FBC', 'pais': 'Chile', 'tipo_calculo': 'D'}  # CORREGIDO: Chile, no Colombia
        }

def process_drapify_with_deduplication(df_drapify):
    """
    Procesa el archivo Drapify eliminando duplicados por order_id
    Solo mantiene la primera ocurrencia de cada order_id
    """
    # Normalizar nombres de columnas
    df_drapify.columns = [to_snake_case(col) for col in df_drapify.columns]
    
    # Verificar duplicados
    duplicates_count = df_drapify['order_id'].duplicated().sum()
    if duplicates_count > 0:
        st.warning(f"⚠️ Se encontraron {duplicates_count} order_id duplicados en Drapify. Se mantendrá solo la primera ocurrencia de cada uno.")
    
    # Eliminar duplicados manteniendo la primera ocurrencia
    df_drapify_clean = df_drapify.drop_duplicates(subset=['order_id'], keep='first').copy()
    
    st.info(f"📊 Órdenes originales: {len(df_drapify)} | Órdenes únicas: {len(df_drapify_clean)}")
    
    return df_drapify_clean

def calcular_utilidades_mejorado(df, store_config, trm_data):
    """
    Calcula utilidades con las reglas de negocio corregidas
    """
    # Asegurar que las columnas básicas existan y sean del tipo correcto
    df['serial'] = df.get('serial', pd.Series(dtype=str)).astype(str)
    df['order_id'] = df.get('order_id', pd.Series(dtype=str)).astype(str)
    df['account_name'] = df.get('account_name', pd.Series(dtype=str)).astype(str)
    
    # Calcular asignación
    df['asignacion'] = df.apply(
        lambda row: calcular_asignacion(row['account_name'], row.get('serial', ''), store_config),
        axis=1
    )
    
    # Mapear país y tipo de cálculo
    df['pais'] = df['account_name'].map(
        lambda x: store_config.get(str(x), {}).get('pais', 'desconocido')
    )
    df['tipo_calculo'] = df['account_name'].map(
        lambda x: store_config.get(str(x), {}).get('tipo_calculo', 'A')
    )
    
    # Mapear moneda
    pais_moneda = {'Colombia': 'COP', 'Perú': 'PEN', 'Peru': 'PEN', 'Chile': 'CLP'}
    df['moneda'] = df['pais'].map(pais_moneda)
    
    # Convertir columnas numéricas importantes
    df['declare_value'] = pd.to_numeric(df.get('declare_value', 0), errors='coerce').fillna(0.0)
    df['quantity'] = pd.to_numeric(df.get('quantity', 1), errors='coerce').fillna(1.0)
    df['net_real_amount'] = pd.to_numeric(df.get('net_real_amount', 0), errors='coerce').fillna(0.0)
    df['logistic_weight_lbs'] = pd.to_numeric(df.get('logistic_weight_lbs', 0), errors='coerce').fillna(0.0)
    
    # CORRECCIÓN 2: Calcular costo_amazon correctamente
    df['costo_amazon'] = df['declare_value'] * df['quantity']
    
    # Calcular meli_usd
    df['meli_usd'] = df.apply(
        lambda row: (row.get('net_real_amount', 0.0) / trm_data.get(row.get('moneda', ''), 1.0))
        if pd.notna(row.get('net_real_amount')) and row.get('moneda') in trm_data else 0.0,
        axis=1
    )
    
    # Inicializar todas las columnas que se usarán en cálculos
    cols_to_init = [
        'total_anican', 'aditional', 'amt_due_cxp', 'arancel_cxp', 'iva_cxp',
        'costo_cxp', 'bodegal', 'socio_cuenta', 'impuesto_facturacion', 
        'gss_logistica', 'impuesto_gss', 'utilidad_gss', 'utilidad_socio'
    ]
    
    for col in cols_to_init:
        df[col] = 0.0
    
    return df

def apply_business_rules_after_merges(df_processed, store_config):
    """
    Aplica las reglas de negocio específicas después de todos los merges
    """
    # CORRECCIÓN 4: bodegal solo para cuentas de Chile
    cuentas_chile = ['2-MEGATIENDA SPA', '8-FABORCARGO']
    df_processed['bodegal'] = df_processed.apply(
        lambda row: 3.5 if (str(row['account_name']) in cuentas_chile and 
                           str(row.get('logistic_type', '')).lower() == 'xd_drop_off') else 0.0,
        axis=1
    )
    
    # CORRECCIÓN 5: socio_cuenta solo para cuentas específicas
    cuentas_socio = ['2-MEGATIENDA SPA', '3-VEENDELO']
    df_processed['socio_cuenta'] = df_processed.apply(
        lambda row: (0.0 if str(row.get('order_status_meli', '')).lower() == 'refunded' else 1.0)
        if str(row['account_name']) in cuentas_socio else 0.0,
        axis=1
    )
    
    # CORRECCIÓN 6: impuesto_facturacion solo para tiendas específicas
    tiendas_impuesto = ['5-DETODOPARATODOS', '6-COMPRAFACIL', '7-COMPRA-YA']
    df_processed['impuesto_facturacion'] = df_processed.apply(
        lambda row: (1.0 if str(row.get('order_status_meli', '')).lower() in ['approved', 'in mediation'] else 0.0)
        if str(row['account_name']) in tiendas_impuesto else 0.0,
        axis=1
    )
    
    # CORRECCIÓN 7: gss_logistica solo para FABORCARGO
    df_processed['peso_kg'] = (df_processed['logistic_weight_lbs'] * df_processed['quantity']) * 0.453592
    df_processed['gss_logistica'] = df_processed.apply(
        lambda row: obtener_gss_logistica(row['peso_kg']) if str(row['account_name']) == '8-FABORCARGO' else 0.0,
        axis=1
    )
    
    # CORRECCIÓN 8: impuesto_gss solo para FABORCARGO
    df_processed['impuesto_gss'] = df_processed.apply(
        lambda row: (row.get('arancel_cxp', 0.0) + row.get('iva_cxp', 0.0))
        if str(row['account_name']) == '8-FABORCARGO' else 0.0,
        axis=1
    )
    
    # CORRECCIÓN 3: Asegurar que costo_cxp se asigne correctamente
    df_processed['costo_cxp'] = df_processed.get('amt_due_cxp', 0.0)
    
    return df_processed

def process_anican_logistics_improved(df_processed, anican_file):
    """
    Procesa el archivo Anican Logistics con mejor manejo de errores
    """
    try:
        df_anican = pd.read_excel(anican_file)
        df_anican.columns = [to_snake_case(col) for col in df_anican.columns]
        
        # Buscar columnas por patrones flexibles
        ref_col = next((col for col in df_anican.columns if 'ref' in col.lower()), None)
        total_col = next((col for col in df_anican.columns if 'total' in col.lower()), None)
        
        if ref_col and total_col:
            # Limpiar y preparar datos
            df_anican[ref_col] = df_anican[ref_col].astype(str).str.strip()
            df_anican[total_col] = pd.to_numeric(df_anican[total_col], errors='coerce').fillna(0.0)
            
            # Crear DataFrame para merge
            anican_merge = df_anican[[ref_col, total_col]].rename(
                columns={ref_col: 'order_id', total_col: 'total_anican_merge'}
            )
            
            # Hacer merge
            df_processed = df_processed.merge(
                anican_merge,
                on='order_id',
                how='left'
            )
            
            # Asignar valores
            df_processed['total_anican'] = df_processed['total_anican_merge'].fillna(0.0)
            df_processed = df_processed.drop(columns=['total_anican_merge'], errors='ignore')
            
            matches = df_processed['total_anican'].gt(0).sum()
            st.success(f"✅ Anican Logistics procesado: {matches} coincidencias encontradas.")
            
        else:
            st.warning(f"⚠️ No se encontraron columnas 'Reference' y 'Total' en Anican Logistics.")
            st.info(f"Columnas disponibles: {list(df_anican.columns)}")
            
    except Exception as e:
        st.error(f"Error procesando Anican Logistics: {str(e)}")
        st.exception(e)
    
    return df_processed

def process_anican_aditionals_improved(df_processed, aditionals_file):
    """
    Procesa el archivo Anican Aditionals con mejor manejo de errores
    """
    try:
        df_aditionals = pd.read_excel(aditionals_file)
        df_aditionals.columns = [to_snake_case(col) for col in df_aditionals.columns]
        
        # Buscar columnas por patrones flexibles
        orderid_col = next((col for col in df_aditionals.columns if 'order' in col.lower() and 'id' in col.lower()), None)
        qty_col = next((col for col in df_aditionals.columns if 'qty' in col.lower() or 'quantity' in col.lower()), None)
        price_col = next((col for col in df_aditionals.columns if 'price' in col.lower() or 'unit' in col.lower()), None)
        
        if orderid_col and qty_col and price_col:
            # Limpiar y preparar datos
            df_aditionals[orderid_col] = df_aditionals[orderid_col].astype(str).str.strip()
            df_aditionals[qty_col] = pd.to_numeric(df_aditionals[qty_col], errors='coerce').fillna(0.0)
            df_aditionals[price_col] = pd.to_numeric(df_aditionals[price_col], errors='coerce').fillna(0.0)
            
            # Calcular total por línea
            df_aditionals['aditional_line_total'] = df_aditionals[qty_col] * df_aditionals[price_col]
            
            # Agrupar por order_id
            aditionals_grouped = df_aditionals.groupby(orderid_col)['aditional_line_total'].sum().reset_index()
            aditionals_grouped = aditionals_grouped.rename(
                columns={orderid_col: 'order_id', 'aditional_line_total': 'aditional_merge'}
            )
            
            # Hacer merge
            df_processed = df_processed.merge(
                aditionals_grouped,
                on='order_id',
                how='left'
            )
            
            # Asignar valores
            df_processed['aditional'] = df_processed['aditional_merge'].fillna(0.0)
            df_processed = df_processed.drop(columns=['aditional_merge'], errors='ignore')
            
            matches = df_processed['aditional'].gt(0).sum()
            st.success(f"✅ Anican Aditionals procesado: {matches} coincidencias encontradas.")
            
        else:
            st.warning(f"⚠️ No se encontraron columnas esperadas en Anican Aditionals.")
            st.info(f"Columnas disponibles: {list(df_aditionals.columns)}")
            
    except Exception as e:
        st.error(f"Error procesando Anican Aditionals: {str(e)}")
        st.exception(e)
    
    return df_processed

def process_cxp_improved(df_processed, cxp_file):
    """
    Procesa el archivo CXP con mejor manejo de errores y detección de columnas
    """
    try:
        excel_file_cxp = pd.ExcelFile(cxp_file)
        df_cxp = None
        
        for sheet_name in excel_file_cxp.sheet_names:
            for header_row in range(5):
                try:
                    df_test_cxp = pd.read_excel(cxp_file, sheet_name=sheet_name, header=header_row)
                    df_test_cxp.columns = [to_snake_case(col) for col in df_test_cxp.columns]
                    
                    # Buscar columnas por patrones flexibles
                    ref_col = next((col for col in df_test_cxp.columns if 'ref' in col.lower()), None)
                    amt_col = next((col for col in df_test_cxp.columns if 'amt' in col.lower() and 'due' in col.lower()), None)
                    arancel_col = next((col for col in df_test_cxp.columns if 'arancel' in col.lower()), None)
                    iva_col = next((col for col in df_test_cxp.columns if 'iva' in col.lower()), None)
                    
                    if ref_col and amt_col:
                        df_cxp = df_test_cxp[[ref_col, amt_col]].copy()
                        df_cxp = df_cxp.rename(columns={ref_col: 'asignacion_cxp', amt_col: 'amt_due_cxp'})
                        
                        # Limpiar y convertir datos
                        df_cxp['asignacion_cxp'] = df_cxp['asignacion_cxp'].astype(str).str.strip()
                        df_cxp['amt_due_cxp'] = pd.to_numeric(df_cxp['amt_due_cxp'], errors='coerce').fillna(0.0)
                        
                        # Agregar columnas adicionales si existen
                        if arancel_col:
                            df_cxp['arancel_cxp'] = pd.to_numeric(df_test_cxp[arancel_col], errors='coerce').fillna(0.0)
                        else:
                            df_cxp['arancel_cxp'] = 0.0
                            
                        if iva_col:
                            df_cxp['iva_cxp'] = pd.to_numeric(df_test_cxp[iva_col], errors='coerce').fillna(0.0)
                        else:
                            df_cxp['iva_cxp'] = 0.0
                        
                        st.info(f"✅ CXP: Columnas detectadas en '{sheet_name}', fila {header_row}")
                        break
                        
                except Exception:
                    continue
                    
            if df_cxp is not None:
                break
        
        if df_cxp is not None:
            # Hacer merge con el DataFrame principal
            df_processed = df_processed.merge(
                df_cxp,
                left_on='asignacion',
                right_on='asignacion_cxp',
                how='left'
            )
            
            # Limpiar columnas temporales
            df_processed = df_processed.drop(columns=['asignacion_cxp'], errors='ignore')
            
            # Asegurar que las columnas existen con valores por defecto
            for col in ['amt_due_cxp', 'arancel_cxp', 'iva_cxp']:
                if col not in df_processed.columns:
                    df_processed[col] = 0.0
                else:
                    df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0.0)
            
            matches = df_processed['amt_due_cxp'].gt(0).sum()
            st.success(f"✅ Chile Express (CXP) procesado: {matches} coincidencias encontradas.")
            
        else:
            st.warning("⚠️ No se pudieron detectar las columnas esperadas en el archivo CXP.")
            
    except Exception as e:
        st.error(f"Error procesando Chile Express (CXP): {str(e)}")
        st.exception(e)
    
    return df_processed

# ============================
# REEMPLAZO PARA LA SECCIÓN DE PROCESAMIENTO EN LA PÁGINA
# ============================

# Este código reemplaza la sección "if st.button("🚀 Procesar y Guardar en Supabase")" 
# en tu página "📁 Procesar Archivos"

if st.button("🚀 Procesar y Guardar en Supabase", type="primary"):
    if drapify_file:
        try:
            with st.spinner("Procesando archivos... Esto puede tardar unos segundos."):
                
                # Usar la configuración actualizada con FABORCARGO corregido
                store_config_updated = get_store_config_updated()
                
                # Procesar todos los archivos con la función mejorada
                df_processed = process_files_improved(
                    drapify_file=drapify_file,
                    anican_file=anican_file,
                    aditionals_file=aditionals_file,
                    cxp_file=cxp_file,
                    store_config=store_config_updated,
                    trm_data=st.session_state.trm_data
                )
                
                # Guardar en session state
                st.session_state.processed_data = df_processed
                st.success("✅ Archivos procesados y utilidades calculadas con éxito!")
                
                # Preparar datos para Supabase (agregación por order_id)
                numeric_cols = [
                    'quantity', 'declare_value', 'net_real_amount', 
                    'logistic_weight_lbs', 'meli_usd', 'costo_amazon', 
                    'total_anican', 'aditional', 'bodegal', 'socio_cuenta', 
                    'costo_cxp', 'impuesto_facturacion', 'gss_logistica', 
                    'impuesto_gss', 'utilidad_gss', 'utilidad_socio', 'peso_kg'
                ]
                
                first_cols = [
                    'account_name', 'serial', 'asignacion', 'pais', 
                    'tipo_calculo', 'moneda', 'logistic_type', 'order_status_meli',
                    'date_created', 'system', 'etiqueta_envio', 'refunded_date'
                ]
                
                # Crear diccionario de agregación
                agg_numeric_cols = {col: 'sum' for col in numeric_cols if col in df_processed.columns}
                agg_first_cols = {col: 'first' for col in first_cols if col in df_processed.columns}
                agg_dict = {**agg_numeric_cols, **agg_first_cols}
                
                # Agregar por order_id para evitar duplicados en Supabase
                df_to_save = df_processed.groupby('order_id').agg(agg_dict).reset_index()
                
                # Guardar en Supabase
                save_success, save_message = save_orders_to_supabase(df_to_save)
                if save_success:
                    st.success(f"💾 {save_message}")
                else:
                    st.error(f"❌ Error al guardar en Supabase: {save_message}")
                
                # Limpiar cache
                st.cache_data.clear()
                
                # Mostrar resumen
                st.markdown("### 📊 Resumen de Procesamiento")
                col_p1, col_p2, col_p3, col_p4 = st.columns(4)
                
                with col_p1:
                    st.metric("Órdenes Únicas Procesadas", len(df_to_save))
                with col_p2:
                    st.metric("Utilidad GSS Total", f"${df_processed['utilidad_gss'].sum():,.2f}")
                with col_p3:
                    st.metric("Utilidad Socio Total", f"${df_processed['utilidad_socio'].sum():,.2f}")
                with col_p4:
                    st.metric("Tiendas Procesadas", df_processed['account_name'].nunique())
                
                # Mostrar estadísticas de coincidencias
                st.markdown("### 🔍 Estadísticas de Coincidencias")
                col_s1, col_s2, col_s3, col_s4 = st.columns(4)
                
                with col_s1:
                    anican_matches = df_processed['total_anican'].gt(0).sum()
                    st.metric("Coincidencias Anican", anican_matches)
                with col_s2:
                    aditional_matches = df_processed['aditional'].gt(0).sum()
                    st.metric("Coincidencias Aditionals", aditional_matches)
                with col_s3:
                    cxp_matches = df_processed['amt_due_cxp'].gt(0).sum()
                    st.metric("Coincidencias CXP", cxp_matches)
                with col_s4:
                    total_records = len(df_processed)
                    st.metric("Total Registros", total_records)
                
                # Vista previa de datos procesados
                st.markdown("### 👀 Vista Previa de Datos Procesados")
                preview_cols = [
                    'order_id', 'account_name', 'pais', 'tipo_calculo', 
                    'meli_usd', 'costo_amazon', 'total_anican', 'aditional',
                    'costo_cxp', 'gss_logistica', 'impuesto_gss', 
                    'utilidad_gss', 'utilidad_socio'
                ]
                available_cols = [col for col in preview_cols if col in df_processed.columns]
                st.dataframe(df_processed[available_cols].head(10), use_container_width=True)
                
                # Verificación de reglas de negocio aplicadas
                st.markdown("### ✅ Verificación de Reglas de Negocio")
                
                # Verificar bodegal solo en Chile
                bodegal_chile = df_processed[df_processed['pais'] == 'Chile']['bodegal'].gt(0).sum()
                bodegal_otros = df_processed[df_processed['pais'] != 'Chile']['bodegal'].gt(0).sum()
                
                if bodegal_otros == 0:
                    st.success(f"✅ Bodegal aplicado correctamente solo en Chile ({bodegal_chile} registros)")
                else:
                    st.warning(f"⚠️ Bodegal aplicado incorrectamente en otros países ({bodegal_otros} registros)")
                
                # Verificar socio_cuenta solo para cuentas específicas
                cuentas_socio = ['2-MEGATIENDA SPA', '3-VEENDELO']
                socio_correcto = df_processed[df_processed['account_name'].isin(cuentas_socio)]['socio_cuenta'].gt(0).sum()
                socio_incorrecto = df_processed[~df_processed['account_name'].isin(cuentas_socio)]['socio_cuenta'].gt(0).sum()
                
                if socio_incorrecto == 0:
                    st.success(f"✅ Socio_cuenta aplicado correctamente solo a cuentas específicas ({socio_correcto} registros)")
                else:
                    st.warning(f"⚠️ Socio_cuenta aplicado incorrectamente a otras cuentas ({socio_incorrecto} registros)")
                
                # Verificar impuesto_facturacion solo para tiendas tipo C
                tiendas_tipo_c = ['5-DETODOPARATODOS', '6-COMPRAFACIL', '7-COMPRA-YA']
                impuesto_correcto = df_processed[df_processed['account_name'].isin(tiendas_tipo_c)]['impuesto_facturacion'].gt(0).sum()
                impuesto_incorrecto = df_processed[~df_processed['account_name'].isin(tiendas_tipo_c)]['impuesto_facturacion'].gt(0).sum()
                
                if impuesto_incorrecto == 0:
                    st.success(f"✅ Impuesto_facturacion aplicado correctamente solo a tiendas tipo C ({impuesto_correcto} registros)")
                else:
                    st.warning(f"⚠️ Impuesto_facturacion aplicado incorrectamente a otras tiendas ({impuesto_incorrecto} registros)")
                
                # Verificar GSS logística solo para FABORCARGO
                gss_faborcargo = df_processed[df_processed['account_name'] == '8-FABORCARGO']['gss_logistica'].gt(0).sum()
                gss_otros = df_processed[df_processed['account_name'] != '8-FABORCARGO']['gss_logistica'].gt(0).sum()
                
                if gss_otros == 0:
                    st.success(f"✅ GSS_logistica aplicado correctamente solo a FABORCARGO ({gss_faborcargo} registros)")
                else:
                    st.warning(f"⚠️ GSS_logistica aplicado incorrectamente a otras cuentas ({gss_otros} registros)")
                
                # Mostrar distribución por país y tipo de cálculo
                st.markdown("### 🌍 Distribución por País y Tipo de Cálculo")
                
                col_dist1, col_dist2 = st.columns(2)
                
                with col_dist1:
                    pais_dist = df_processed['pais'].value_counts()
                    st.write("**Distribución por País:**")
                    for pais, count in pais_dist.items():
                        st.write(f"• {pais}: {count} órdenes")
                
                with col_dist2:
                    tipo_dist = df_processed['tipo_calculo'].value_counts()
                    st.write("**Distribución por Tipo de Cálculo:**")
                    for tipo, count in tipo_dist.items():
                        st.write(f"• Tipo {tipo}: {count} órdenes")
                
                # Verificar país de FABORCARGO
                faborcargo_pais = df_processed[df_processed['account_name'] == '8-FABORCARGO']['pais'].iloc[0] if len(df_processed[df_processed['account_name'] == '8-FABORCARGO']) > 0 else "No encontrado"
                
                if faborcargo_pais == "Chile":
                    st.success("✅ FABORCARGO correctamente asignado a Chile")
                else:
                    st.error(f"❌ FABORCARGO incorrectamente asignado a: {faborcargo_pais}")
                
                # Opción para descargar datos procesados
                csv = df_processed.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Descargar Datos Procesados (CSV)",
                    data=csv,
                    file_name=f"datos_contables_procesados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )
                
        except Exception as e:
            st.error(f"❌ Error durante el procesamiento: {str(e)}")
            st.exception(e)
    else:
        st.warning("⚠️ Por favor, carga el archivo DRAPIFY para iniciar el procesamiento.")

# ============================
# FUNCIONES AUXILIARES ADICIONALES
# ============================

def obtener_gss_logistica(peso_kg):
    """Obtiene valor de GSS según peso - función corregida"""
    ANEXO_A = [
        (0.01, 0.50, 24.01), (0.51, 1.00, 33.09), (1.01, 1.50, 42.17), (1.51, 2.00, 51.25),
        (2.01, 2.50, 61.94), (2.51, 3.00, 71.02), (3.01, 3.50, 80.91), (3.51, 4.00, 89.99),
        (4.01, 4.50, 99.87), (4.51, 5.00, 99.87), (5.01, 5.50, 108.95), (5.51, 6.00, 117.19),
        (6.01, 6.50, 126.12), (6.51, 7.00, 135.85), (7.01, 7.50, 144.78), (7.51, 8.00, 154.52),
        (8.01, 8.50, 163.75), (8.51, 9.00, 173.18), (9.01, 9.50, 182.11), (9.51, 10.00, 191.85),
        (10.01, 10.50, 200.78), (10.51, 11.00, 207.36), (11.01, 11.50, 216.14), (11.51, 12.00, 225.73),
        (12.01, 12.50, 234.51), (12.51, 13.00, 244.09), (13.01, 13.50, 252.87), (13.51, 14.00, 262.46),
        (14.01, 14.50, 271.24), (14.51, 15.00, 280.82), (15.01, 15.50, 289.60), (15.51, 16.00, 294.54),
        (16.01, 16.50, 303.17), (16.51, 17.00, 312.60), (17.01, 17.50, 321.23), (17.51, 18.00, 330.67),
        (18.01, 18.50, 339.30), (18.51, 19.00, 348.73), (19.01, 19.50, 357.36), (19.51, 20.00, 366.80),
        (20.01, float('inf'), 373.72)
    ]
    
    if pd.isna(peso_kg) or peso_kg <= 0:
        return 0
    
    for desde, hasta, gss_value in ANEXO_A:
        if desde <= peso_kg <= hasta:
            return gss_value
    
    return 0

def calcular_asignacion(account_name, serial_number, store_config):
    """Calcula la columna Asignacion"""
    if pd.isna(account_name) or pd.isna(serial_number):
        return None
    
    account_str = str(account_name).strip()
    prefijo = store_config.get(account_str, {}).get('prefijo', '')
    
    if prefijo:
        return f"{prefijo}{serial_number}"
    return None

def process_files_improved(drapify_file, anican_file=None, aditionals_file=None, cxp_file=None, store_config=None, trm_data=None):
    """
    Función principal mejorada para procesar todos los archivos
    """
    try:
        # 1. PROCESAR DRAPIFY CON DEDUPLICACIÓN
        st.info("🔄 Procesando archivo Drapify...")
        df_drapify = pd.read_excel(drapify_file)
        df_processed = process_drapify_with_deduplication(df_drapify)
        
        # 2. CALCULAR UTILIDADES BASE
        df_processed = calcular_utilidades_mejorado(df_processed, store_config, trm_data)
        
        # 3. PROCESAR ARCHIVOS ADICIONALES
        if anican_file:
            st.info("🔄 Procesando Anican Logistics...")
            df_processed = process_anican_logistics_improved(df_processed, anican_file)
        
        if aditionals_file:
            st.info("🔄 Procesando Anican Aditionals...")
            df_processed = process_anican_aditionals_improved(df_processed, aditionals_file)
        
        if cxp_file:
            st.info("🔄 Procesando Chile Express (CXP)...")
            df_processed = process_cxp_improved(df_processed, cxp_file)
        
        # 4. APLICAR REGLAS DE NEGOCIO FINALES
        st.info("🔄 Aplicando reglas de negocio...")
        df_processed = apply_business_rules_after_merges(df_processed, store_config)
        
        # 5. CALCULAR UTILIDADES FINALES
        df_processed = calculate_final_profits(df_processed)
        
        return df_processed
        
    except Exception as e:
        st.error(f"Error en el procesamiento: {str(e)}")
        raise e

def calculate_final_profits(df):
    """
    Calcula las utilidades finales según el tipo de cálculo
    """
    def apply_profit_calculation(row):
        tipo = row.get('tipo_calculo', 'A')
        
        meli_usd = row.get('meli_usd', 0.0)
        costo_amazon = row.get('costo_amazon', 0.0)
        total_anican = row.get('total_anican', 0.0)
        aditional = row.get('aditional', 0.0)
        costo_cxp = row.get('costo_cxp', 0.0)
        bodegal = row.get('bodegal', 0.0)
        socio_cuenta = row.get('socio_cuenta', 0.0)
        impuesto_facturacion = row.get('impuesto_facturacion', 0.0)
        gss_logistica = row.get('gss_logistica', 0.0)
        impuesto_gss = row.get('impuesto_gss', 0.0)
        amt_due_cxp = row.get('amt_due_cxp', 0.0)
        
        utilidad_gss_final = 0.0
        utilidad_socio_final = 0.0
        
        if tipo == 'A':
            utilidad_gss_final = meli_usd - costo_amazon - total_anican - aditional
        elif tipo == 'B':
            utilidad_gss_final = meli_usd - costo_cxp - costo_amazon - bodegal - socio_cuenta
        elif tipo == 'C':
            utilidad_base_c = meli_usd - costo_amazon - total_anican - aditional - impuesto_facturacion
            if utilidad_base_c > 7.5:
                utilidad_socio_final = 7.5
                utilidad_gss_final = utilidad_base_c - 7.5
            else:
                utilidad_socio_final = utilidad_base_c
                utilidad_gss_final = 0.0
        elif tipo == 'D':
            utilidad_gss_final = gss_logistica + impuesto_gss - amt_due_cxp
        
        return pd.Series([utilidad_gss_final, utilidad_socio_final], 
                        index=['utilidad_gss', 'utilidad_socio'])
    
    df[['utilidad_gss', 'utilidad_socio']] = df.apply(apply_profit_calculation, axis=1)
    return df
