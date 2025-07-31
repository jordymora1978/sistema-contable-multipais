import streamlit as st
import pandas as pd
import numpy as np
from supabase import create_client, Client
import os
from datetime import datetime, timedelta
import io
import time

# IMPORTS PARA UTILIDADES (solo si el módulo existe)
try:
    from modulo_utilidades import get_calculador_utilidades
    import plotly.express as px
    import plotly.graph_objects as go
    UTILIDADES_AVAILABLE = True
except ImportError:
    UTILIDADES_AVAILABLE = False

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
    url = "https://qzexuqkedukcwcyhrpza.supabase.co"
    key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"
    return create_client(url, key)

supabase = init_supabase()

def verificar_conexion_supabase():
    if not supabase:
        return False, "No se pudo inicializar la conexión"
    try:
        result = supabase.table('orders').select('id').limit(1).execute()
        return True, "Conexión exitosa"
    except Exception as e:
        return False, str(e)

# PÁGINAS DE LA APLICACIÓN
def mostrar_consolidador(processing_mode):
    st.title("🏠 Consolidador de Archivos")
    st.info("Funcionalidad de consolidador de archivos")

def mostrar_calculo_utilidades():
    st.title("💰 Cálculo de Utilidades")
    if not UTILIDADES_AVAILABLE:
        st.warning("⚠️ Módulo de utilidades no disponible")
        return
    try:
        calculador = get_calculador_utilidades()
        st.success("✅ Módulo de utilidades cargado")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("🇨🇴 Colombia", f"${calculador.trm_actual.get('colombia', 0):,.2f}")
        with col2:
            st.metric("🇵🇪 Perú", f"${calculador.trm_actual.get('peru', 0):,.2f}")
        with col3:
            st.metric("🇨🇱 Chile", f"${calculador.trm_actual.get('chile', 0):,.2f}")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")

def mostrar_gestion_trm():
    """GESTIÓN MANUAL DE TRM"""
    st.title("💱 Gestión de TRM")
    st.markdown("### Control Manual de Tasas Representativas del Mercado")
    
    if not UTILIDADES_AVAILABLE:
        st.warning("⚠️ Módulo de utilidades no disponible")
        return
    
    try:
        calculador = get_calculador_utilidades()
        
        st.info("💡 **Importante:** Cambiar la TRM afecta TODOS los cálculos futuros de utilidades.")
        
        # CONFIGURACIÓN MANUAL DE TRM
        st.subheader("⚙️ Configurar TRM Manualmente")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**🇨🇴 COLOMBIA**")
            nueva_trm_colombia = st.number_input(
                "TRM Colombia (COP/USD):",
                value=float(calculador.trm_actual.get('colombia', 4250.0)),
                min_value=1000.0,
                max_value=10000.0,
                step=0.01,
                format="%.2f"
            )
            st.caption(f"Actual: ${calculador.trm_actual.get('colombia', 0):,.2f}")
        
        with col2:
            st.markdown("**🇵🇪 PERÚ**")
            nueva_trm_peru = st.number_input(
                "TRM Perú (PEN/USD):",
                value=float(calculador.trm_actual.get('peru', 3.75)),
                min_value=1.0,
                max_value=10.0,
                step=0.01,
                format="%.2f"
            )
            st.caption(f"Actual: ${calculador.trm_actual.get('peru', 0):,.2f}")
        
        with col3:
            st.markdown("**🇨🇱 CHILE**")
            nueva_trm_chile = st.number_input(
                "TRM Chile (CLP/USD):",
                value=float(calculador.trm_actual.get('chile', 850.0)),
                min_value=500.0,
                max_value=1500.0,
                step=0.01,
                format="%.2f"
            )
            st.caption(f"Actual: ${calculador.trm_actual.get('chile', 0):,.2f}")
        
        # BOTÓN PARA ACTUALIZAR
        st.markdown("---")
        if st.button("💾 ACTUALIZAR TRM", type="primary", use_container_width=True):
            nuevas_trm = {
                'colombia': nueva_trm_colombia,
                'peru': nueva_trm_peru,
                'chile': nueva_trm_chile
            }
            
            with st.spinner("Actualizando TRM..."):
                if calculador.actualizar_trm(nuevas_trm, "usuario_manual"):
                    st.success("✅ ¡TRM actualizada exitosamente!")
                    st.balloons()
                    st.rerun()
        
        # HISTORIAL
        st.subheader("📋 Últimos Cambios")
        try:
            result = supabase.table('trm_history').select('*').order('fecha_cambio', desc=True).limit(5).execute()
            if result.data:
                historial_df = pd.DataFrame(result.data)
                st.dataframe(historial_df, use_container_width=True)
            else:
                st.info("No hay cambios registrados")
        except Exception as e:
            st.error(f"Error: {str(e)}")
            
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")

def mostrar_dashboard_utilidades():
    st.title("📊 Dashboard de Utilidades")
    st.info("📝 No hay datos de utilidades para mostrar")

def mostrar_reportes():
    st.title("📋 Reportes")
    st.info("🚧 Funcionalidad en desarrollo")

# FUNCIÓN PRINCIPAL
def main():
    st.title("💰 Sistema de Gestión Integral")
    st.markdown("### Consolidación de archivos y cálculo de utilidades")
    
    conexion_ok, mensaje_conexion = verificar_conexion_supabase()
    
    # Sidebar
    with st.sidebar:
        st.image("https://via.placeholder.com/150x50/4F46E5/white?text=LOGO", width=150)
        st.markdown("---")
        
        if conexion_ok:
            st.success("✅ Supabase conectado")
        else:
            st.error("❌ Sin conexión BD")
        
        st.markdown("---")
        
        # NAVEGACIÓN SIEMPRE CON 5 OPCIONES
        pagina = st.selectbox("📋 Navegación", [
            "🏠 Consolidador de Archivos",
            "💰 Cálculo de Utilidades",
            "💱 Gestión TRM",
            "📊 Dashboard Utilidades", 
            "📋 Reportes"
        ])
        
        st.markdown("---")
        processing_mode = st.radio("Modo:", ["Solo consolidar", "Consolidar e insertar en DB"])
    
    # ROUTING
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

if __name__ == "__main__":
    main()
