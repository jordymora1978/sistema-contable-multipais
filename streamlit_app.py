st.info(f"Ref='{ref_col}', Amt='{amt_col}', Arancel='{arancel_col}', IVA='{iva_col}'")
                        
                        # Crear DataFrame para merge
                        
                        df_cxp['asignacion_cxp'] = df_test_cxp[ref_col].astype(str).str.strip()
                        df_cxp['amt_due_cxp'] = pd.to_numeric(df_test_cxp[amt_col], errors='coerce').fillna(0.0)
                        
                        if arancel_col:
                            df_cxp['arancel_cxp'] = pd.to_numeric(df_test_cxp[arancel_col], errors='coerce').fillna(0.0)
                        else:
                            df_cxp['arancel_cxp'] = 0.0
                            
                        if iva_col:
                            df_cxp['iva_cxp'] = pd.to_numeric(df_test_cxp[iva_col], errors='coerce').fillna(0.0)
                        else:
                            df_cxp['iva_cxp'] = 0.0
                        
                        break
                        
                except Exception as e:
                    continue
                    
            if df_cxp is not None:
                break
        
        if df_cxp is not None:
            # Hacer merge usando asignacion
            df_processed = df_processed.merge(
                df_cxp,
                left_on='asignacion',
                right_on='asignacion_cxp',
                how='left'
            )
            
            # Limpiar columna temporal
            df_processed = df_processed.drop(columns=['asignacion_cxp'])
            
            # Rellenar NaN con 0
            for col in ['amt_due_cxp', 'arancel_cxp', 'iva_cxp']:
                df_processed[col] = df_processed[col].fillna(0.0)
            
            matches = df_processed['amt_due_cxp'].gt(0).sum()
            st.success(f"✅ Chile Express (CXP) procesado: {matches} coincidencias encontradas")
            
        else:
            st.warning("⚠️ No se pudieron detectar las columnas esperadas en el archivo CXP")
            # Inicializar columnas con 0 si no se procesó CXP
            for col in ['amt_due_cxp', 'arancel_cxp', 'iva_cxp']:
                df_processed[col] = 0.0
                
    except Exception as e:
        st.error(f"❌ Error procesando Chile Express (CXP): {str(e)}")
        # Inicializar columnas con 0 en caso de error
        for col in ['amt_due_cxp', 'arancel_cxp', 'iva_cxp']:
            if col not in df_processed.columns:
                df_processed[col] = 0.0
    
    return df_processed

def apply_business_rules(df_processed):
    """Aplica reglas de negocio específicas según las correcciones solicitadas"""
    
    # CORRECCIÓN: bodegal solo para cuentas de Chile
    cuentas_chile = ['2-MEGATIENDA SPA', '8-FABORCARGO']
    df_processed['bodegal'] = df_processed.apply(
        lambda row: 3.5 if (str(row['account_name']) in cuentas_chile and 
                           str(row.get('logistic_type', '')).lower() == 'xd_drop_off') else 0.0,
        axis=1
    )
    
    # CORRECCIÓN: socio_cuenta solo para cuentas específicas
    cuentas_socio = ['2-MEGATIENDA SPA', '3-VEENDELO']
    df_processed['socio_cuenta'] = df_processed.apply(
        lambda row: (0.0 if str(row.get('order_status_meli', '')).lower() == 'refunded' else 1.0)
        if str(row['account_name']) in cuentas_socio else 0.0,
        axis=1
    )
    
    # CORRECCIÓN: impuesto_facturacion solo para tiendas específicas
    tiendas_impuesto = ['5-DETODOPARATODOS', '6-COMPRAFACIL', '7-COMPRA-YA']
    df_processed['impuesto_facturacion'] = df_processed.apply(
        lambda row: (1.0 if str(row.get('order_status_meli', '')).lower() in ['approved', 'in mediation'] else 0.0)
        if str(row['account_name']) in tiendas_impuesto else 0.0,
        axis=1
    )
    
    # Calcular peso en kg
    df_processed['peso_kg'] = (df_processed['logistic_weight_lbs'] * df_processed['quantity']) * 0.453592
    
    # CORRECCIÓN: gss_logistica solo para FABORCARGO
    df_processed['gss_logistica'] = df_processed.apply(
        lambda row: obtener_gss_logistica(row['peso_kg']) if str(row['account_name']) == '8-FABORCARGO' else 0.0,
        axis=1
    )
    
    # CORRECCIÓN: impuesto_gss solo para FABORCARGO
    df_processed['impuesto_gss'] = df_processed.apply(
        lambda row: (row.get('arancel_cxp', 0.0) + row.get('iva_cxp', 0.0))
        if str(row['account_name']) == '8-FABORCARGO' else 0.0,
        axis=1
    )
    
    # CORRECCIÓN: costo_cxp = amt_due_cxp
    df_processed['costo_cxp'] = df_processed['amt_due_cxp']
    
    return df_processed

def calculate_final_profits(df):
    """Calcula utilidades finales según tipo de cálculo"""
    def apply_profit_calculation(row):
        tipo = row.get('tipo_calculo', 'A')
        
        meli_usd = float(row.get('meli_usd', 0.0))
        costo_amazon = float(row.get('costo_amazon', 0.0))
        total_anican = float(row.get('total_anican', 0.0))
        aditional = float(row.get('aditional', 0.0))
        costo_cxp = float(row.get('costo_cxp', 0.0))
        bodegal = float(row.get('bodegal', 0.0))
        socio_cuenta = float(row.get('socio_cuenta', 0.0))
        impuesto_facturacion = float(row.get('impuesto_facturacion', 0.0))
        gss_logistica = float(row.get('gss_logistica', 0.0))
        impuesto_gss = float(row.get('impuesto_gss', 0.0))
        amt_due_cxp = float(row.get('amt_due_cxp', 0.0))
        
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

# ============================
# INICIALIZAR SESSION STATE
# ============================

if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None

if 'trm_data' not in st.session_state:
    st.session_state.trm_data = get_trm_rates()
    st.session_state.trm_data['last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if 'store_config' not in st.session_state:
    st.session_state.store_config = get_store_config()

# ============================
# INTERFAZ PRINCIPAL
# ============================

# Header
st.markdown("""
<div class="main-header">
    <h1>🌎 Sistema Contable Multi-País</h1>
    <p>Versión Corregida - Todas las Mejoras Implementadas</p>
</div>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.title("🎛️ Panel de Control")

# Estado de conexión
st.sidebar.markdown("### 🔗 Estado de Conexión")
connected, message = test_connection()
if connected:
    st.sidebar.success(message)
else:
    st.sidebar.error(message)

# Navegación
page = st.sidebar.selectbox("Selecciona una sección:", [
    "🏠 Inicio",
    "📊 Dashboard en Tiempo Real",
    "📁 Procesar Archivos",
    "💱 Configurar TRM",
    "📋 Fórmulas de Negocio"
])

# TRM actual
st.sidebar.markdown("### 💱 TRM Actual")
for currency, rate in st.session_state.trm_data.items():
    if currency != 'last_update':
        st.sidebar.metric(f"{currency}/USD", f"{rate:,.2f}")

# ============================
# PÁGINAS
# ============================

if page == "🏠 Inicio":
    st.header("🏠 Bienvenido al Sistema Contable")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <h3>📊 Dashboard</h3>
            <p>Visualiza utilidades por mes usando date_created</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <h3>📁 Procesar</h3>
            <p>Todas las correcciones implementadas</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card">
            <h3>✅ Sin Errores</h3>
            <p>Manejo robusto de archivos y columnas</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Estadísticas rápidas
    try:
        success, df = get_orders_from_supabase(100)
        
        if success and len(df) > 0:
            st.markdown("### 📈 Resumen Rápido (Últimas 100 órdenes)")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📦 Total Órdenes", len(df))
            with col2:
                total_utilidad = df['utilidad_gss'].sum() + df['utilidad_socio'].sum()
                st.metric("Utilidad Total", f"${total_utilidad:,.2f}")
            with col3:
                paises = df['pais'].nunique()
                st.metric("Países Activos", paises)
            with col4:
                tiendas = df['account_name'].nunique()
                st.metric("Tiendas Activas", tiendas)
        else:
            st.markdown("""
            <div class="info-box">
                <h4>🚀 ¡Comienza aquí!</h4>
                <p>No hay datos en Supabase aún. Ve a <strong>"Procesar Archivos"</strong> para cargar tu primera información.</p>
            </div>
            """, unsafe_allow_html=True)
        
    except Exception as e:
        st.warning(f"⚠️ No se pudo cargar resumen: {str(e)}")

elif page == "📊 Dashboard en Tiempo Real":
    st.header("📊 Dashboard en Tiempo Real")
    
    try:
        success, df = get_orders_from_supabase(1000)
        
        if success and len(df) > 0:
            # Asegurar que date_created es datetime
            df['date_created'] = pd.to_datetime(df['date_created'], errors='coerce')
            df = df.dropna(subset=['date_created'])
            
            # Agregar columna de mes para agrupación
            df['mes'] = df['date_created'].dt.to_period('M').astype(str)
            
            # Filtros
            st.markdown("### 🔍 Filtros")
            col_f1, col_f2, col_f3 = st.columns(3)
            
            with col_f1:
                paises_disponibles = ['Todos'] + list(df['pais'].unique())
                pais_seleccionado = st.selectbox("País", paises_disponibles)
            
            with col_f2:
                meses_disponibles = ['Todos'] + sorted(df['mes'].unique(), reverse=True)
                mes_seleccionado = st.selectbox("Mes", meses_disponibles)
            
            with col_f3:
                tiendas_disponibles = ['Todas'] + list(df['account_name'].unique())
                tienda_seleccionada = st.selectbox("Tienda", tiendas_disponibles)
            
            # Aplicar filtros
            df_filtrado = df.copy()
            if pais_seleccionado != 'Todos':
                df_filtrado = df_filtrado[df_filtrado['pais'] == pais_seleccionado]
            if mes_seleccionado != 'Todos':
                df_filtrado = df_filtrado[df_filtrado['mes'] == mes_seleccionado]
            if tienda_seleccionada != 'Todas':
                df_filtrado = df_filtrado[df_filtrado['account_name'] == tienda_seleccionada]
            
            # Métricas principales
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📦 Órdenes Filtradas", len(df_filtrado))
            with col2:
                utilidad_gss = df_filtrado['utilidad_gss'].sum()
                st.metric("🏢 Utilidad GSS", f"${utilidad_gss:,.2f}")
            with col3:
                utilidad_socio = df_filtrado['utilidad_socio'].sum()
                st.metric("🤝 Utilidad Socio", f"${utilidad_socio:,.2f}")
            with col4:
                total_ingresos = df_filtrado['meli_usd'].sum()
                st.metric("💰 Ingresos Total", f"${total_ingresos:,.2f}")
            
            # Gráficos
            st.markdown("### 📊 Análisis Visual")
            
            tab1, tab2, tab3 = st.tabs(["Por Mes", "Por País", "Por Tienda"])
            
            with tab1:
                # Utilidades por mes
                mes_data = df.groupby('mes').agg({
                    'utilidad_gss': 'sum',
                    'utilidad_socio': 'sum',
                    'order_id': 'count'
                }).reset_index()
                
                fig_mes = px.bar(
                    mes_data, 
                    x='mes', 
                    y=['utilidad_gss', 'utilidad_socio'],
                    title='Utilidades Mensuales (USD)',
                    height=400,
                    labels={'value': 'Utilidad (USD)', 'variable': 'Tipo de Utilidad'}
                )
                st.plotly_chart(fig_mes, use_container_width=True)
            
            with tab2:
                # Utilidades por país
                pais_data = df_filtrado.groupby('pais').agg({
                    'utilidad_gss': 'sum',
                    'utilidad_socio': 'sum',
                    'order_id': 'count'
                }).reset_index()
                
                fig_pais = px.pie(
                    pais_data, 
                    values='utilidad_gss', 
                    names='pais',
                    title='Distribución de Utilidad GSS por País'
                )
                st.plotly_chart(fig_pais, use_container_width=True)
            
            with tab3:
                # Top tiendas
                tienda_data = df_filtrado.groupby('account_name').agg({
                    'utilidad_gss': 'sum',
                    'utilidad_socio': 'sum'
                }).reset_index()
                tienda_data['total'] = tienda_data['utilidad_gss'] + tienda_data['utilidad_socio']
                tienda_data = tienda_data.sort_values('total', ascending=True).tail(10)
                
                fig_tienda = px.bar(
                    tienda_data, 
                    x='total', 
                    y='account_name',
                    orientation='h',
                    title='Top 10 Tiendas por Utilidad Total',
                    height=500
                )
                st.plotly_chart(fig_tienda, use_container_width=True)
            
            # Tabla de datos
            st.markdown("### 📋 Datos Detallados")
            st.dataframe(
                df_filtrado[['order_id', 'account_name', 'pais', 'mes', 'meli_usd', 'utilidad_gss', 'utilidad_socio']].head(20),
                use_container_width=True
            )
        
        else:
            st.warning("No hay datos disponibles en Supabase para mostrar en el Dashboard.")
    
    except Exception as e:
        st.error(f"Error cargando dashboard: {str(e)}")

elif page == "📁 Procesar Archivos":
    st.header("📁 Cargar y Procesar Archivos")
    
    st.markdown("""
    <div class="info-box">
        <h4>🔄 Proceso Corregido</h4>
        <p>Todas las mejoras implementadas: deduplicación, detección exacta de columnas, reglas específicas por tienda.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Uploaders
    col_drapify, col_otros = st.columns(2)

    with col_drapify:
        st.markdown("### 📊 Archivo Principal: DRAPIFY")
        drapify_file = st.file_uploader(
            "📄 Cargar DRAPIFY (Orders_XXXXXXXX)",
            type=['xlsx', 'xls'],
            key="drapify_uploader",
            help="Archivo principal con todas las órdenes de MercadoLibre."
        )

    with col_otros:
        st.markdown("### 🚚 Archivos Adicionales")
        anican_file = st.file_uploader(
            "🚚 Cargar Anican Logistics",
            type=['xlsx', 'xls'],
            key="anican_uploader",
            help="Archivo con costos logísticos de Anican (columnas: Reference, Total)."
        )
        aditionals_file = st.file_uploader(
            "➕ Cargar Anican Aditionals",
            type=['xlsx', 'xls'],
            key="aditionals_uploader",
            help="Archivo con costos adicionales (columnas: Order Id, Quantity, UnitPrice)."
        )
        cxp_file = st.file_uploader(
            "🇨🇱 Cargar Chile Express (CXP)",
            type=['xlsx', 'xls'],
            key="cxp_uploader",
            help="Archivo de Chile Express con costos logísticos y de aduana."
        )
    
    if st.button("🚀 Procesar y Guardar en Supabase", type="primary"):
        if drapify_file:
            try:
                with st.spinner("Procesando archivos... Esto puede tardar unos segundos."):
                    
                    # 1. PROCESAR ARCHIVO DRAPIFY
                    st.info("🔄 Paso 1: Procesando archivo Drapify...")
                    df_drapify = pd.read_excel(drapify_file)
                    df_processed = process_drapify_file(df_drapify)
                    
                    # 2. CALCULAR CAMPOS BÁSICOS
                    st.info("🔄 Paso 2: Calculando campos básicos...")
                    df_processed = calculate_basic_fields(df_processed, st.session_state.store_config, st.session_state.trm_data)
                    
                    # 3. PROCESAR ARCHIVOS ADICIONALES
                    if anican_file:
                        st.info("🔄 Paso 3a: Procesando Anican Logistics...")
                        df_processed = process_anican_logistics(df_processed, anican_file)
                    
                    if aditionals_file:
                        st.info("🔄 Paso 3b: Procesando Anican Aditionals...")
                        df_processed = process_anican_aditionals(df_processed, aditionals_file)
                    
                    if cxp_file:
                        st.info("🔄 Paso 3c: Procesando Chile Express (CXP)...")
                        df_processed = process_cxp_file(df_processed, cxp_file)
                    
                    # 4. APLICAR REGLAS DE NEGOCIO
                    st.info("🔄 Paso 4: Aplicando reglas de negocio específicas...")
                    df_processed = apply_business_rules(df_processed)
                    
                    # 5. CALCULAR UTILIDADES FINALES
                    st.info("🔄 Paso 5: Calculando utilidades finales...")
                    df_processed = calculate_final_profits(df_processed)
                    
                    # Guardar en session state
                    st.session_state.processed_data = df_processed
                    st.success("✅ Archivos procesados y utilidades calculadas con éxito!")

                    # PREPARAR DATOS PARA SUPABASE - AGREGAR POR ORDER_ID
                    st.info("🔄 Paso 6: Preparando datos para Supabase...")
                    
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
                    
                    # Crear diccionario de agregación solo con columnas que existen
                    agg_numeric_cols = {col: 'sum' for col in numeric_cols if col in df_processed.columns}
                    agg_first_cols = {col: 'first' for col in first_cols if col in df_processed.columns}
                    agg_dict = {**agg_numeric_cols, **agg_first_cols}
                    
                    # Agregar por order_id para evitar duplicados en Supabase
                    df_to_save = df_processed.groupby('order_id').agg(agg_dict).reset_index()
                    
                    # GUARDAR EN SUPABASE
                    st.info("🔄 Paso 7: Guardando en Supabase...")
                    save_success, save_message = save_orders_to_supabase(df_to_save)
                    
                    if save_success:
                        st.success(f"💾 {save_message}")
                    else:
                        st.error(f"❌ Error al guardar en Supabase: {save_message}")
                    
                    # Limpiar cache para actualizar dashboard
                    st.cache_data.clear()
                    
                    # MOSTRAR RESUMEN
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
                    
                    # VERIFICACIONES DE REGLAS DE NEGOCIO
                    st.markdown("### ✅ Verificación de Reglas de Negocio")
                    
                    # Verificar FABORCARGO país
                    faborcargo_data = df_processed[df_processed['account_name'] == '8-FABORCARGO']
                    if len(faborcargo_data) > 0:
                        faborcargo_pais = faborcargo_data['pais'].iloc[0]
                        if faborcargo_pais == "Chile":
                            st.success("✅ FABORCARGO correctamente asignado a Chile")
                        else:
                            st.error(f"❌ FABORCARGO incorrectamente asignado a: {faborcargo_pais}")
                    
                    # Verificar bodegal solo en Chile
                    bodegal_chile = df_processed[df_processed['pais'] == 'Chile']['bodegal'].gt(0).sum()
                    bodegal_otros = df_processed[df_processed['pais'] != 'Chile']['bodegal'].gt(0).sum()
                    
                    if bodegal_otros == 0:
                        st.success(f"✅ Bodegal aplicado correctamente solo en Chile ({bodegal_chile} registros)")
                    else:
                        st.warning(f"⚠️ Bodegal aplicado incorrectamente en otros países ({bodegal_otros} registros)")
                    
                    # Verificar GSS logística solo para FABORCARGO
                    gss_faborcargo = df_processed[df_processed['account_name'] == '8-FABORCARGO']['gss_logistica'].gt(0).sum()
                    gss_otros = df_processed[df_processed['account_name'] != '8-FABORCARGO']['gss_logistica'].gt(0).sum()
                    
                    if gss_otros == 0:
                        st.success(f"✅ GSS_logistica aplicado correctamente solo a FABORCARGO ({gss_faborcargo} registros)")
                    else:
                        st.warning(f"⚠️ GSS_logistica aplicado incorrectamente a otras cuentas ({gss_otros} registros)")
                    
                    # MOSTRAR ESTADÍSTICAS DE COINCIDENCIAS
                    st.markdown("### 🔍 Estadísticas de Coincidencias")
                    col_s1, col_s2, col_s3 = st.columns(3)
                    
                    with col_s1:
                        anican_matches = df_processed['total_anican'].gt(0).sum()
                        st.metric("Coincidencias Anican", anican_matches)
                    with col_s2:
                        aditional_matches = df_processed['aditional'].gt(0).sum()
                        st.metric("Coincidencias Aditionals", aditional_matches)
                    with col_s3:
                        cxp_matches = df_processed['amt_due_cxp'].gt(0).sum()
                        st.metric("Coincidencias CXP", cxp_matches)
                    
                    # VISTA PREVIA DE DATOS
                    st.markdown("### 👀 Vista Previa de Datos Procesados")
                    preview_cols = [
                        'order_id', 'account_name', 'pais', 'tipo_calculo', 
                        'meli_usd', 'costo_amazon', 'total_anican', 'aditional',
                        'costo_cxp', 'gss_logistica', 'utilidad_gss', 'utilidad_socio'
                    ]
                    available_cols = [col for col in preview_cols if col in df_processed.columns]
                    st.dataframe(df_processed[available_cols].head(10), use_container_width=True)

                    # OPCIÓN DE DESCARGA
                    csv = df_processed.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Descargar Datos Procesados (CSV)",
                        data=csv,
                        file_name=f"datos_contables_procesados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                    )
            
            except Exception as e:
                st.error(f"❌ Ocurrió un error durante el procesamiento: {str(e)}")
                st.exception(e)
        else:
            st.warning("⚠️ Por favor, carga el archivo DRAPIFY para iniciar el procesamiento.")

elif page == "💱 Configurar TRM":
    st.header("💱 Configuración de Tasas de Cambio")
    
    # Recargar TRM actual
    st.session_state.trm_data = get_trm_rates()

    with st.form("trm_form"):
        st.markdown("### Tasas de Cambio Actuales (USD a Moneda Local)")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("### 🇨🇴 Colombia")
            cop_trm = st.number_input(
                "COP por 1 USD",
                value=float(st.session_state.trm_data.get('COP', 4000.0)),
                step=50.0,
                min_value=1000.0,
                max_value=10000.0,
                format="%.2f"
            )
        
        with col2:
            st.markdown("### 🇵🇪 Perú")
            pen_trm = st.number_input(
                "PEN por 1 USD",
                value=float(st.session_state.trm_data.get('PEN', 3.8)),
                step=0.01,
                min_value=1.0,
                max_value=10.0,
                format="%.3f"
            )
        
        with col3:
            st.markdown("### 🇨🇱 Chile")
            clp_trm = st.number_input(
                "CLP por 1 USD",
                value=float(st.session_state.trm_data.get('CLP', 900.0)),
                step=10.0,
                min_value=500.0,
                max_value=1500.0,
                format="%.2f"
            )
        
        submitted = st.form_submit_button("💾 Actualizar TRM", type="primary")
    
    if submitted:
        try:
            new_trm_data = {
                'COP': cop_trm,
                'PEN': pen_trm,
                'CLP': clp_trm,
            }
            
            success, message = save_trm_rates(new_trm_data)
            
            if success:
                st.success("✅ Tasas TRM actualizadas exitosamente y guardadas en Supabase!")
                st.session_state.trm_data = get_trm_rates() 
                st.session_state.trm_data['last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.rerun()
            else:
                st.error(f"❌ Error al guardar TRM en Supabase: {message}")
            
        except Exception as e:
            st.error(f"❌ Error actualizando TRM: {str(e)}")

elif page == "📋 Fórmulas de Negocio":
    st.header("📋 Fórmulas de Negocio por Tipo de Tienda")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Tipo A", "Tipo B", "Tipo C", "Tipo D"])
    
    with tab1:
        st.markdown("### Tipo A: TODOENCARGO-CO y MEGA TIENDAS PERUANAS")
        st.markdown("""
        <div class="formula-box">
        <h4>📐 Fórmula Principal</h4>
        <strong>Utilidad Gss = MELI USD - Costo Amazon - Total Anican - Aditional</strong><br><br>
        
        <h5>🔧 Componentes:</h5>
        • <strong>Costo Amazon:</strong> Declare Value × quantity<br>
        • <strong>Total Anican:</strong> del archivo Anican Logistics (columna Total)<br>
        • <strong>Aditional:</strong> Quantity × UnitPrice (del archivo Aditionals)<br>
        • <strong>MELI USD:</strong> net_real_amount / TRM<br><br>
        
        <h5>🌍 Países aplicables:</h5>
        • Colombia (TODOENCARGO-CO)<br>
        • Perú (MEGA TIENDAS PERUANAS)
        </div>
        """, unsafe_allow_html=True)
    
    with tab2:
        st.markdown("### Tipo B: MEGATIENDA SPA y VEENDELO")
        st.markdown("""
        <div class="formula-box">
        <h4>📐 Fórmula Principal</h4>
        <strong>Utilidad Gss = MELI USD - Costo cxp - Costo Amazon - Bodegal - Socio_cuenta</strong><br><br>
        
        <h5>🔧 Componentes:</h5>
        • <strong>Costo cxp:</strong> Amt. Due (del archivo Chile Express CXP)<br>
        • <strong>Bodegal:</strong> 3.5 si logistic_type = "xd_drop_off" Y cuenta es de Chile<br>
        • <strong>Socio_cuenta:</strong> 0 si order_status_meli = "refunded", sino 1 (solo para estas cuentas)<br>
        • <strong>Asignacion:</strong> prefijo + Serial# para unir con CXP<br><br>
        
        <h5>🌍 Países aplicables:</h5>
        • Chile (MEGATIENDA SPA)<br>
        • Colombia (VEENDELO)
        </div>
        """, unsafe_allow_html=True)
    
    with tab3:
        st.markdown("### Tipo C: DETODOPARATODOS, COMPRAFACIL, COMPRA-YA")
        st.markdown("""
        <div class="formula-box">
        <h4>📐 Fórmula Principal</h4>
        <strong>Utilidad Gss = MELI USD - Costo Amazon - Total Anican - Aditional - Impuesto por facturación</strong><br><br>
        
        <h5>🔧 Lógica Especial:</h5>
        • <strong>Impuesto por facturación:</strong> 1 si order_status_meli = "approved" o "in mediation" (solo para estas tiendas)<br>
        • <strong>Utilidad Socio:</strong> 7.5 si Utilidad > 7.5, sino Utilidad<br>
        • <strong>Si Utilidad > 7.5:</strong> Utilidad Gss = Utilidad - Utilidad Socio<br>
        • <strong>Si Utilidad ≤ 7.5:</strong> Utilidad Gss = 0<br><br>
        
        <h5>🌍 Países aplicables:</h5>
        • Colombia (todas las tiendas tipo C)
        </div>
        """, unsafe_allow_html=True)
    
    with tab4:
        st.markdown("### Tipo D: FABORCARGO")
        st.markdown("""
        <div class="formula-box">
        <h4>📐 Fórmula Principal</h4>
        <strong>Utilidad Gss = Gss Logística + Impuesto Gss - Amt. Due</strong><br><br>
        
        <h5>🔧 Componentes:</h5>
        • <strong>País:</strong> Chile (CORREGIDO)<br>
        • <strong>Peso:</strong> logistic_weight_lbs × quantity × 0.453592 (conversión a kg)<br>
        • <strong>Gss Logística:</strong> según tabla ANEXO A por peso en kg (solo FABORCARGO)<br>
        • <strong>Impuesto Gss:</strong> Arancel + IVA del archivo CXP (solo FABORCARGO)<br>
        • <strong>Bodegal:</strong> 3.5 si logistic_type = "xd_drop_off" (solo cuentas Chile)<br><br>
        
        <h5>🌍 Países aplicables:</h5>
        • Chile (FABORCARGO)
        </div>
        """, unsafe_allow_html=True)
    
    # Mostrar correcciones implementadas
    st.markdown("---")
    st.markdown("### ✅ Correcciones Implementadas")
    
    correcciones = [
        "✅ Deduplicación automática por order_id en Drapify",
        "✅ costo_amazon = Declare Value × quantity",
        "✅ Detección exacta de columnas: Reference, Total, Order Id, Quantity, UnitPrice",
        "✅ FABORCARGO asignado correctamente a Chile",
        "✅ costo_cxp = Amt. Due del archivo CXP",
        "✅ impuesto_facturacion solo para COMPRAFACIL, DETODOPARATODOS, COMPRA-YA",
        "✅ gss_logistica e impuesto_gss solo para FABORCARGO",
        "✅ socio_cuenta solo para MEGATIENDA SPA y VEENDELO",
        "✅ bodegal solo para cuentas de Chile",
        "✅ Sin columnas duplicadas (_x, _y)",
        "✅ Manejo robusto de errores en detección de columnas"
    ]
    
    for correccion in correcciones:
        st.markdown(correccion)

# ============================
# FOOTER
# ============================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 1rem;'>
    <p><strong>Sistema Contable Multi-País v4.0</strong> | Powered by Streamlit + Supabase</p>
    <p>🌎 Todas las correcciones implementadas | Dashboard con filtros por mes usando date_created</p>
</div>
""", unsafe_allow_html=True)import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
import time
import re

# ============================
# CONFIGURACIÓN INICIAL
# ============================

st.set_page_config(
    page_title="Sistema Contable Multi-País",
    page_icon="🌎",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado
st.markdown("""
<style>
.main-header {
    background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    padding: 2rem;
    border-radius: 1rem;
    color: white;
    text-align: center;
    margin-bottom: 2rem;
}

.metric-card {
    background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
    padding: 1.5rem;
    border-radius: 1rem;
    text-align: center;
    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    border-left: 5px solid #007bff;
}

.success-box {
    background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #28a745;
    margin: 1rem 0;
}

.info-box {
    background: linear-gradient(135deg, #d1ecf1 0%, #bee5eb 100%);
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #17a2b8;
    margin: 1rem 0;
}

.warning-box {
    background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%);
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #ffc107;
    margin: 1rem 0;
}

.formula-box {
    background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
    padding: 1.5rem;
    border-radius: 1rem;
    border-left: 5px solid #28a745;
    margin: 1rem 0;
    box-shadow: 0 2px 10px rgba(0,0,0,0.05);
}

.stButton > button {
    width: 100%;
    border-radius: 0.5rem;
    height: 3rem;
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)

# ============================
# FUNCIONES SUPABASE
# ============================

@st.cache_resource
def init_supabase():
    """Inicializa conexión con Supabase"""
    try:
        url = st.secrets.get("supabase", {}).get("url", "https://qzexuqkedukcwcyhrpza.supabase.co")
        key = st.secrets.get("supabase", {}).get("key", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0")
        
        supabase: Client = create_client(url, key)
        return supabase
    except Exception as e:
        st.error(f"Error conectando a Supabase: {str(e)}")
        return None

def test_connection():
    """Prueba la conexión con Supabase"""
    try:
        supabase = init_supabase()
        if supabase:
            result = supabase.table('trm_rates').select('count').execute() 
            return True, "✅ Conectado a Supabase"
        return False, "❌ No se pudo inicializar Supabase"
    except Exception as e:
        return False, f"❌ Error de conexión: {str(e)}"

@st.cache_data(ttl=3600)
def get_store_config():
    """Obtiene configuración de tiendas desde Supabase"""
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
        
    # Configuración por defecto CORREGIDA
    return {
        '1-TODOENCARGO-CO': {'prefijo': 'TDC', 'pais': 'Colombia', 'tipo_calculo': 'A'},
        '2-MEGATIENDA SPA': {'prefijo': 'MEGA', 'pais': 'Chile', 'tipo_calculo': 'B'},
        '3-VEENDELO': {'prefijo': 'VEEN', 'pais': 'Colombia', 'tipo_calculo': 'B'},
        '4-MEGA TIENDAS PERUANAS': {'prefijo': 'MGA-PE', 'pais': 'Perú', 'tipo_calculo': 'A'},
        '5-DETODOPARATODOS': {'prefijo': 'DTPT', 'pais': 'Colombia', 'tipo_calculo': 'C'},
        '6-COMPRAFACIL': {'prefijo': 'CFA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
        '7-COMPRA-YA': {'prefijo': 'CPYA', 'pais': 'Colombia', 'tipo_calculo': 'C'},
        '8-FABORCARGO': {'prefijo': 'FBC', 'pais': 'Chile', 'tipo_calculo': 'D'}  # CORREGIDO: Chile
    }

@st.cache_data(ttl=600)
def get_trm_rates():
    """Obtiene las últimas tasas TRM"""
    try:
        supabase = init_supabase()
        if supabase:
            try:
                result = supabase.table('trm_rates').select('*').order('created_at', desc=True).execute()
            except Exception:
                result = supabase.table('trm_rates').select('*').order('id', desc=True).execute()
                
            if result.data:
                df_trm = pd.DataFrame(result.data)
                for col_date in ['date_updated', 'created_at']:
                    if col_date in df_trm.columns:
                        df_trm[col_date] = pd.to_datetime(df_trm[col_date], errors='coerce')
                
                if 'id' in df_trm.columns:
                    df_trm['id'] = pd.to_numeric(df_trm['id'], errors='coerce')
                    df_trm = df_trm.dropna(subset=['id'])

                if 'created_at' in df_trm.columns and not df_trm['created_at'].isnull().all():
                    df_trm = df_trm.sort_values(by=['currency', 'created_at'], ascending=[True, False])
                else:
                    df_trm = df_trm.sort_values(by=['currency', 'id'], ascending=[True, False])
                
                df_trm = df_trm.drop_duplicates(subset=['currency'], keep='first')
                return {row['currency']: row['rate'] for _, row in df_trm.iterrows()}
        
        return {'COP': 4000.0, 'PEN': 3.8, 'CLP': 900.0}
    except Exception as e:
        st.error(f"Error obteniendo TRM: {str(e)}")
        return {'COP': 4000.0, 'PEN': 3.8, 'CLP': 900.0}

def save_trm_rates(trm_data):
    """Guarda tasas TRM en Supabase"""
    try:
        supabase = init_supabase()
        if supabase:
            records_to_insert = []
            for currency, rate in trm_data.items():
                if currency != 'last_update':
                    records_to_insert.append({
                        'currency': currency,
                        'rate': float(rate),
                        'updated_by': 'streamlit_app',
                        'date_updated': datetime.now().isoformat()
                    })
            if records_to_insert:
                result = supabase.table('trm_rates').insert(records_to_insert).execute()
                return True, "TRM guardado exitosamente"
            return False, "No hay datos TRM para guardar"
        return False, "No se pudo conectar a Supabase"
    except Exception as e:
        return False, f"Error al guardar TRM: {str(e)}"

def save_orders_to_supabase(df_processed_for_save):
    """Guarda órdenes procesadas en Supabase"""
    try:
        supabase = init_supabase()
        if not supabase:
            return False, "No hay conexión a Supabase"
        
        orders_data = []
        for _, row in df_processed_for_save.iterrows():
            order_dict = {
                'order_id': str(row.get('order_id', '')),
                'account_name': str(row.get('account_name', '')),
                'serial_number': str(row.get('serial', '')),
                'asignacion': str(row.get('asignacion', '')),
                'pais': str(row.get('pais', '')),
                'tipo_calculo': str(row.get('tipo_calculo', '')),
                'moneda': str(row.get('moneda', '')),
                'date_created': row.get('date_created').isoformat() if pd.notna(row.get('date_created')) and isinstance(row.get('date_created'), datetime) else None,
                'quantity': int(row.get('quantity', 0)),
                'logistic_type': str(row.get('logistic_type', '')),
                'order_status_meli': str(row.get('order_status_meli', '')),
                'declare_value': float(row.get('declare_value', 0)),
                'net_real_amount': float(row.get('net_real_amount', 0)),
                'logistic_weight_lbs': float(row.get('logistic_weight_lbs', 0)),
                'meli_usd': float(row.get('meli_usd', 0)),
                'costo_amazon': float(row.get('costo_amazon', 0)),
                'total_anican': float(row.get('total_anican', 0)),
                'aditional': float(row.get('aditional', 0)),
                'bodegal': float(row.get('bodegal', 0)),
                'socio_cuenta': float(row.get('socio_cuenta', 0)),
                'costo_cxp': float(row.get('costo_cxp', 0)),
                'impuesto_facturacion': float(row.get('impuesto_facturacion', 0)),
                'gss_logistica': float(row.get('gss_logistica', 0)),
                'impuesto_gss': float(row.get('impuesto_gss', 0)),
                'utilidad_gss': float(row.get('utilidad_gss', 0)),
                'utilidad_socio': float(row.get('utilidad_socio', 0))
            }
            orders_data.append(order_dict)
            
        # Insertar en lotes
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(orders_data), batch_size):
            batch = orders_data[i:i+batch_size]
            result = supabase.table('orders').upsert(batch, on_conflict='order_id').execute()
            total_inserted += len(batch)
            
        return True, f"{total_inserted} órdenes guardadas/actualizadas"
        
    except Exception as e:
        return False, f"Error al guardar órdenes: {str(e)}"

@st.cache_data(ttl=60)
def get_orders_from_supabase(limit=1000):
    """Obtiene órdenes desde Supabase"""
    try:
        supabase = init_supabase()
        if supabase:
            try:
                result = supabase.table('orders').select('*').order('created_at', desc=True).limit(limit).execute()
            except Exception:
                result = supabase.table('orders').select('*').order('order_id', desc=True).limit(limit).execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                for col_date in ['date_created', 'created_at']:
                    if col_date in df.columns:
                        df[col_date] = pd.to_datetime(df[col_date], errors='coerce')
                return True, df
        return False, "No hay datos en la tabla orders"
    except Exception as e:
        return False, pd.DataFrame()

# ============================
# FUNCIONES DE NEGOCIO
# ============================

def to_snake_case(name):
    """Función auxiliar para normalizar nombres de columnas a snake_case"""
    name = str(name)
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    name = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', name)
    return name.lower().replace(' ', '_').replace('.', '').replace('#', '').replace('-', '_')

def calcular_asignacion(account_name, serial_number, store_config):
    """Calcula la columna Asignacion"""
    if pd.isna(account_name) or pd.isna(serial_number):
        return None
    
    account_str = str(account_name).strip()
    prefijo = store_config.get(account_str, {}).get('prefijo', '')
    
    if prefijo:
        return f"{prefijo}{serial_number}"
    return None

def obtener_gss_logistica(peso_kg):
    """Obtiene valor de GSS según peso"""
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

def process_drapify_file(df_drapify):
    """Procesa el archivo Drapify eliminando duplicados por order_id"""
    # Normalizar nombres de columnas
    df_drapify.columns = [to_snake_case(col) for col in df_drapify.columns]
    
    # Verificar duplicados por order_id
    duplicates_count = df_drapify['order_id'].duplicated().sum()
    if duplicates_count > 0:
        st.warning(f"⚠️ Se encontraron {duplicates_count} order_id duplicados en Drapify. Se mantendrá solo la primera ocurrencia.")
    
    # Eliminar duplicados manteniendo la primera ocurrencia
    df_clean = df_drapify.drop_duplicates(subset=['order_id'], keep='first').copy()
    
    st.info(f"📊 Órdenes originales: {len(df_drapify)} | Órdenes únicas: {len(df_clean)}")
    
    return df_clean

def calculate_basic_fields(df, store_config, trm_data):
    """Calcula campos básicos y utilidades iniciales"""
    # Asegurar tipos de datos básicos
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
    
    # CORRECCIÓN: Calcular costo_amazon correctamente
    df['costo_amazon'] = df['declare_value'] * df['quantity']
    
    # Calcular meli_usd
    df['meli_usd'] = df.apply(
        lambda row: (row.get('net_real_amount', 0.0) / trm_data.get(row.get('moneda', ''), 1.0))
        if pd.notna(row.get('net_real_amount')) and row.get('moneda') in trm_data else 0.0,
        axis=1
    )
    
    # Inicializar columnas que se llenarán en los merges
    init_cols = [
        'total_anican', 'aditional', 'amt_due_cxp', 'arancel_cxp', 'iva_cxp',
        'costo_cxp', 'bodegal', 'socio_cuenta', 'impuesto_facturacion', 
        'gss_logistica', 'impuesto_gss', 'utilidad_gss', 'utilidad_socio', 'peso_kg'
    ]
    
    for col in init_cols:
        df[col] = 0.0
    
    return df

def process_anican_logistics(df_processed, anican_file):
    """Procesa archivo Anican Logistics con detección mejorada de columnas"""
    try:
        df_anican = pd.read_excel(anican_file)
        
        st.info(f"🔍 Columnas detectadas en Anican Logistics: {list(df_anican.columns)}")
        
        # Buscar columna Reference (no normalizar aún para mostrar original)
        reference_col = None
        total_col = None
        
        for col in df_anican.columns:
            if str(col).strip().lower() == 'reference':
                reference_col = col
            elif str(col).strip().lower() == 'total':
                total_col = col
        
        if reference_col and total_col:
            st.success(f"✅ Columnas encontradas: Reference='{reference_col}', Total='{total_col}'")
            
            # Preparar datos para merge
            df_anican_clean = df_anican[[reference_col, total_col]].copy()
            df_anican_clean[reference_col] = df_anican_clean[reference_col].astype(str).str.strip()
            df_anican_clean[total_col] = pd.to_numeric(df_anican_clean[total_col], errors='coerce').fillna(0.0)
            
            # Renombrar para merge
            df_anican_clean = df_anican_clean.rename(columns={
                reference_col: 'order_id',
                total_col: 'total_anican_temp'
            })
            
            # Hacer merge
            df_processed = df_processed.merge(
                df_anican_clean,
                on='order_id',
                how='left'
            )
            
            # Asignar valores
            df_processed['total_anican'] = df_processed['total_anican_temp'].fillna(0.0)
            df_processed = df_processed.drop(columns=['total_anican_temp'])
            
            matches = df_processed['total_anican'].gt(0).sum()
            st.success(f"✅ Anican Logistics procesado: {matches} coincidencias encontradas")
            
        else:
            st.error(f"❌ No se encontraron columnas 'Reference' y 'Total' en Anican Logistics")
            st.info("Columnas disponibles: " + ", ".join(df_anican.columns))
            
    except Exception as e:
        st.error(f"❌ Error procesando Anican Logistics: {str(e)}")
    
    return df_processed

def process_anican_aditionals(df_processed, aditionals_file):
    """Procesa archivo Anican Aditionals con detección mejorada de columnas"""
    try:
        df_aditionals = pd.read_excel(aditionals_file)
        
        st.info(f"🔍 Columnas detectadas en Anican Aditionals: {list(df_aditionals.columns)}")
        
        # Buscar columnas exactas
        order_id_col = None
        quantity_col = None
        unitprice_col = None
        
        for col in df_aditionals.columns:
            if str(col).strip().lower() == 'order id':
                order_id_col = col
            elif str(col).strip().lower() == 'quantity':
                quantity_col = col
            elif str(col).strip().lower() == 'unitprice':
                unitprice_col = col
        
        if order_id_col and quantity_col and unitprice_col:
            st.success(f"✅ Columnas encontradas: Order Id='{order_id_col}', Quantity='{quantity_col}', UnitPrice='{unitprice_col}'")
            
            # Preparar datos
            df_aditionals_clean = df_aditionals[[order_id_col, quantity_col, unitprice_col]].copy()
            df_aditionals_clean[order_id_col] = df_aditionals_clean[order_id_col].astype(str).str.strip()
            df_aditionals_clean[quantity_col] = pd.to_numeric(df_aditionals_clean[quantity_col], errors='coerce').fillna(0.0)
            df_aditionals_clean[unitprice_col] = pd.to_numeric(df_aditionals_clean[unitprice_col], errors='coerce').fillna(0.0)
            
            # Calcular total por línea
            df_aditionals_clean['line_total'] = df_aditionals_clean[quantity_col] * df_aditionals_clean[unitprice_col]
            
            # Agrupar por order_id
            aditionals_grouped = df_aditionals_clean.groupby(order_id_col)['line_total'].sum().reset_index()
            aditionals_grouped = aditionals_grouped.rename(columns={
                order_id_col: 'order_id',
                'line_total': 'aditional_temp'
            })
            
            # Hacer merge
            df_processed = df_processed.merge(
                aditionals_grouped,
                on='order_id',
                how='left'
            )
            
            # Asignar valores
            df_processed['aditional'] = df_processed['aditional_temp'].fillna(0.0)
            df_processed = df_processed.drop(columns=['aditional_temp'])
            
            matches = df_processed['aditional'].gt(0).sum()
            st.success(f"✅ Anican Aditionals procesado: {matches} coincidencias encontradas")
            
        else:
            st.error(f"❌ No se encontraron columnas 'Order Id', 'Quantity' y 'UnitPrice' en Anican Aditionals")
            st.info("Columnas disponibles: " + ", ".join(df_aditionals.columns))
            
    except Exception as e:
        st.error(f"❌ Error procesando Anican Aditionals: {str(e)}")
    
    return df_processed

def process_cxp_file(df_processed, cxp_file):
    """Procesa archivo CXP con detección mejorada y sin duplicar columnas"""
    try:
        # LIMPIAR columnas CXP existentes antes del merge para evitar duplicados
        cxp_cols_to_clean = ['amt_due_cxp', 'arancel_cxp', 'iva_cxp']
        for col in cxp_cols_to_clean:
            if col in df_processed.columns:
                df_processed = df_processed.drop(columns=[col])
        
        excel_file_cxp = pd.ExcelFile(cxp_file)
        df_cxp = None
        
        for sheet_name in excel_file_cxp.sheet_names:
            for header_row in range(5):
                try:
                    df_test_cxp = pd.read_excel(cxp_file, sheet_name=sheet_name, header=header_row)
                    
                    st.info(f"🔍 Probando hoja '{sheet_name}', fila {header_row}. Columnas: {list(df_test_cxp.columns)}")
                    
                    # Buscar columnas exactas
                    ref_col = None
                    amt_col = None
                    arancel_col = None
                    iva_col = None
                    
                    for col in df_test_cxp.columns:
                        col_str = str(col).strip().lower()
                        if 'ref' in col_str and '#' in col_str:
                            ref_col = col
                        elif 'amt' in col_str and 'due' in col_str:
                            amt_col = col
                        elif 'arancel' in col_str:
                            arancel_col = col
                        elif 'iva' in col_str:
                            iva_col = col
                    
                    if ref_col and amt_col:
                        st.success(f"✅ CXP: Columnas encontradas en '{sheet_name}', fila {header_row}")
                        st.info(f"Ref='{ref
