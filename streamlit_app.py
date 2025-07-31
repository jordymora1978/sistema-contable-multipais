# BOTÓN DE PROCESAMIENTO - Siempre habilitado si hay archivo Drapify
button_text = "🔄 Reprocesar Archivos" if st.session_state.processing_complete else "🚀 Procesar Archivos"
if st.button(button_text, disabled=not drapify_file, type="primary"):

    with st.spinner("Procesando archivos..."):
        try:
            # Leer archivo Drapify
            if drapify_file and drapify_file.name.endswith('.csv'):
                drapify_df = pd.read_csv(drapify_file)
            elif drapify_file:
                drapify_df = pd.read_excel(drapify_file)
            else:
                st.error("❌ No se encontró archivo Drapify")
                return

            st.success(f"✅ Drapify cargado: {len(drapify_df)} registros")

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

            # Procesar consolidación
            consolidated_df = process_files_according_to_rules(
                drapify_df, logistics_df, aditionals_df, cxp_df, logistics_date
            )

            # Aplicar formateos
            st.header("🎨 Aplicando Formateos")
            consolidated_df = apply_formatting(consolidated_df)

            # GUARDAR EN SESSION STATE (sobrescribir datos anteriores)
            st.session_state.consolidated_data = consolidated_df
            st.session_state.processing_complete = True
            st.session_state.last_processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            st.session_state.processing_stats = {
                'total_registros': len(consolidated_df),
                'asignaciones': consolidated_df['asignacion'].notna().sum() if 'asignacion' in consolidated_df.columns else 0,
                'con_fecha_logistics': consolidated_df['fecha_logistics'].notna().sum() if 'fecha_logistics' in consolidated_df.columns else 0,
                'columnas': len(consolidated_df.columns)
            }

            # Limpiar utilidades anteriores al reprocesar
            st.session_state.utilidades_data = None
            st.session_state.utilidades_calculated = False

            st.success("💾 Datos guardados en memoria (datos anteriores reemplazados)")

            # INSERCIÓN AUTOMÁTICA EN BASE DE DATOS
            st.header("💾 Insertando en Base de Datos")

            with st.spinner("Validando duplicados e insertando datos..."):
                result = insert_to_supabase_with_validation(consolidated_df)

            # Mostrar resultados detallados
            st.subheader("📊 Resultado de la Inserción")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("📥 Nuevos Insertados", result['inserted'],
                                 delta=f"+{result['inserted']}" if result['inserted'] > 0 else None)

            with col2:
                st.metric("⚠️ Duplicados", result['duplicates'],
                                 delta="Ya existían" if result['duplicates'] > 0 else None)

            with col3:
                st.metric("❌ Errores", result['errors'],
                                 delta="No insertados" if result['errors'] > 0 else None)

            with col4:
                total_procesados = result['inserted'] + result['duplicates'] + result['errors']
                st.metric("📋 Total Procesados", total_procesados)

            # Mensajes de resultado
            if result['inserted'] > 0:
                st.success(f"✅ {result['inserted']} registros nuevos insertados correctamente")

            if result['duplicates'] > 0:
                st.warning(f"⚠️ {result['duplicates']} registros ya existían en la base de datos")

            if result['errors'] > 0:
                st.error(f"❌ {result['errors']} registros tuvieron errores al insertar")

            if result['inserted'] > 0:
                st.balloons()

            # Verificar total en BD
            try:
                total_bd_result = supabase.table('orders').select('id', count='exact').execute()
                total_bd = total_bd_result.count
                st.info(f"📊 Total registros en base de datos: **{total_bd:,}**")
            except Exception as e:
                st.warning("No se pudo verificar el total en BD")

        except Exception as e:
            st.error(f"❌ Error procesando archivos: {str(e)}")
            st.exception(e)
