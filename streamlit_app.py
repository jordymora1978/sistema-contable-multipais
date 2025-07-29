import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client, Client

# Configuración Supabase
SUPABASE_URL = "https://qzexuqkedukcwcyhrpza.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Configuración de la app
st.set_page_config(page_title="Sistema Contable Multi-País", layout="wide")
st.markdown("""
    <style>
        .main {background-color: #0e1117;}
        .stApp {background-color: #0e1117; color: white;}
        .stSelectbox > div {color: black !important;}
        .stDataFrame {background-color: white; color: black;}
    </style>
""", unsafe_allow_html=True)

# Sidebar - Panel de Control
st.sidebar.title("🧿 Panel de Control")
menu = st.sidebar.selectbox("Selecciona una sección:", ["Dashboard", "Configurar TRM", "Procesar Archivos", "Análisis de Utilidad", "Fórmulas de Negocio"])

# Función: Obtener TRMs actuales
def obtener_trm():
    try:
        result = supabase.table("trm_rates").select("*").order("id", desc=True).execute()
        df = pd.DataFrame(result.data)
        if 'date_updated' in df.columns:
            df['date_updated'] = pd.to_datetime(df['date_updated'], format='ISO8601', errors='coerce')
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], format='ISO8601', errors='coerce')
        return df
    except Exception as e:
        st.error("❌ Error al leer datos de Supabase.")
        st.exception(e)
        return pd.DataFrame()

# Página: Configurar TRM
if menu == "Configurar TRM":
    st.header("🔍 Probando conexión con Supabase...")
    df_trm = obtener_trm()
    if not df_trm.empty:
        st.success("✅ Datos de TRM obtenidos con éxito")
        st.dataframe(df_trm)

    st.subheader("🛠️ Agregar o Editar TRM")
    moneda = st.text_input("Moneda (ej: COP, PEN, CLP)")
    tasa = st.number_input("Tasa", min_value=0.0001, format="%.4f")
    actualizado_por = st.text_input("Actualizado por", value="jordy_mora")
    editar = st.checkbox("Editar si ya existe")

    if st.button("💾 Guardar TRM"):
        try:
            fecha_actual = datetime.now().isoformat()
            if editar:
                # Buscar ID más reciente de esa moneda
                registros = df_trm[df_trm['currency'].str.lower() == moneda.lower()]
                if not registros.empty:
                    ultimo_id = registros.iloc[0]['id']
                    response = supabase.table("trm_rates").update({
                        "rate": tasa,
                        "updated_by": actualizado_por,
                        "date_updated": fecha_actual
                    }).eq("id", ultimo_id).execute()
                    st.success("✅ TRM actualizada con éxito")
                else:
                    st.warning("⚠️ No se encontró TRM previa para editar. Agregando nueva...")
                    supabase.table("trm_rates").insert({
                        "currency": moneda,
                        "rate": tasa,
                        "updated_by": actualizado_por,
                        "date_updated": fecha_actual
                    }).execute()
                    st.success("✅ TRM agregada con éxito")
            else:
                supabase.table("trm_rates").insert({
                    "currency": moneda,
                    "rate": tasa,
                    "updated_by": actualizado_por,
                    "date_updated": fecha_actual
                }).execute()
                st.success("✅ TRM agregada con éxito")
        except Exception as e:
            st.error("❌ Error inesperado al guardar TRM.")
            st.exception(e)

# Página: Procesar Archivos
elif menu == "Procesar Archivos":
    st.header("📂 Cargar y Procesar Archivos")
    st.write("Sube los archivos necesarios. El sistema calculará las utilidades según el tipo de tienda:")
    st.markdown("""
    - **Tipo A**: TODOENCARGO-CO, MEGA TIENDAS PERUANAS  
    - **Tipo B**: MEGATIENDA SPA, VEENDELO  
    - **Tipo C**: DETODOPARATODOS, COMPRAFACIL, COMPRA-YA  
    - **Tipo D**: FABORCARGO
    """)

    st.subheader("📁 Archivos Principales")
    principal_file = st.file_uploader("DRAPIFY (Orders_XXXXXX)", type=["xlsx", "xls"])

    st.subheader("🚚 Archivos Logísticos")
    cxp_file = st.file_uploader("Chile Express (CXP)", type=["xlsx", "xls"])
    anican_file = st.file_uploader("Anican Logistics", type=["xlsx", "xls"])
    add_file = st.file_uploader("Anican Aditionals", type=["xlsx", "xls"])

    if st.button("🧮 Procesar y Calcular Utilidades"):
        st.warning("⚠️ Esta sección aún no está implementada. En desarrollo...")

# Página: Dashboard
elif menu == "Dashboard":
    st.header("📊 Dashboard General")
    st.info("⚠️ Esta sección está en desarrollo.")

# Página: Análisis de Utilidad
elif menu == "Análisis de Utilidad":
    st.header("📈 Análisis de Utilidad")
    st.info("⚠️ Módulo en construcción para análisis avanzados por tienda.")

# Página: Fórmulas de Negocio
elif menu == "Fórmulas de Negocio":
    st.header("📐 Fórmulas de Negocio")
    st.info("⚠️ Próximamente podrás configurar márgenes y reglas por país.")
