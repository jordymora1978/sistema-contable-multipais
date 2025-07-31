# ===============================================
# MODIFICACIONES PARA AGREGAR UTILIDADES
# Agregar estas partes a tu streamlit_app.py existente
# ===============================================

# 1. AGREGAR IMPORT AL INICIO (después de tus imports existentes)
from modulo_utilidades import get_calculador_utilidades
import plotly.express as px
import plotly.graph_objects as go

# 2. MODIFICAR EL SIDEBAR (reemplazar tu sidebar actual)
def main():
    # ... tu código existente ...
    
    # SIDEBAR MODIFICADO
    with st.sidebar:
        st.image("https://via.placeholder.com/150x50/4F46E5/white?text=LOGO", width=150)
        st.markdown("---")
        
        # NAVEGACIÓN EXPANDIDA (agregar esta página)
        pagina = st.selectbox(
            "📋 Navegación",
            [
                "🏠 Consolidador de Archivos",  # Tu página actual
                "💰 Cálculo de Utilidades",    # NUEVA PÁGINA
                "💱 Gestión TRM",              # NUEVA PÁGINA  
                "📊 Dashboard Utilidades",     # NUEVA PÁGINA
                "📋 Reportes"                  # NUEVA PÁGINA
            ]
        )
        
        st.markdown("---")
        
        # Estado del sistema
        st.markdown("### 📊 Estado del Sistema")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("🟢 Online", "99.8%")
        with col2:
            st.metric("📈 Uptime", "24/7")
    
    # 3. ROUTING DE PÁGINAS (agregar después de tu código actual)
    if pagina == "🏠 Consolidador de Archivos":
        # TU CÓDIGO ACTUAL DEL CONSOLIDADOR VA AQUÍ
        mostrar_consolidador()  # Mover tu código actual a esta función
    
    elif pagina == "💰 Cálculo de Utilidades":
        mostrar_calculo_utilidades()
    
    elif pagina == "💱 Gestión TRM":
        mostrar_gestion_trm()
    
    elif pagina == "📊 Dashboard Utilidades":
        mostrar_dashboard_utilidades()
    
    elif pagina == "📋 Reportes":
        mostrar_reportes()

# 4. FUNCIÓN CONSOLIDADOR (mover tu código actual aquí)
def mostrar_consolidador():
    """Tu código actual del consolidador va aquí completo"""
    # COPIAR AQUÍ TODO TU CÓDIGO ACTUAL desde st.title hasta el final
    # (todo lo que está después del sidebar)
    st.title("📦 Consolidador de Órdenes")
    # ... resto de tu código actual ...

# 5. NUEVAS FUNCIONES PARA UTILIDADES
def mostrar_calculo_utilidades():
    """Página principal de cálculo de utilidades"""
    st.title("💰 Cálculo de Utilidades")
    st.markdown("### Procesamiento automático según reglas de negocio")
    
    # Obtener calculador
    calculador = get_calculador_utilidades()
    
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
                value=1000,
                step=50
            )
        
        with col3:
            solo_sin_utilidades = st.checkbox(
                "Solo órdenes sin utilidades calculadas",
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
    
    with tab2:
        st.subheader("📊 Resultados Guardados")
        
        try:
            # Obtener estadísticas
            stats_df = calculador.obtener_estadisticas_cuenta()
            
            if not stats_df.empty:
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
    
    calculador = get_calculador_utilidades()
    
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
        historial_df = calculador.obtener_historial_trm(dias=30)
        
        if not historial_df.empty:
            st.dataframe(historial_df, use_container_width=True)
            
            # Gráfico de evolución
            if len(historial_df) > 1:
                fig = px.line(historial_df, x='fecha_cambio', y='valor_nuevo', 
                            color='pais', title="Evolución TRM últimos 30 días")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📝 No hay cambios registrados en los últimos 30 días")
    
    except Exception as e:
        st.error(f"❌ Error cargando historial: {str(e)}")

def mostrar_dashboard_utilidades():
    """Dashboard de utilidades"""
    st.title("📊 Dashboard de Utilidades")
    st.markdown("### Panel de control y métricas")
    
    calculador = get_calculador_utilidades()
    
    try:
        # Obtener estadísticas
        stats_df = calculador.obtener_estadisticas_cuenta()
        
        if not stats_df.empty:
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
    st.info("🚧 Funcionalidad en desarrollo - Próximamente disponible")

# IMPORTANTE: Al final, asegúrate de que tu main() se ejecute
if __name__ == "__main__":
    main()
