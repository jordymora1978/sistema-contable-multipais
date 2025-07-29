import streamlit as st
from supabase import create_client, Client
import pandas as pd
from datetime import datetime

# 🔐 Claves de conexión a Supabase (pegadas directamente)
SUPABASE_URL = "https://qzexuqkedukcwcyhrpza.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

st.set_page_config(page_title="TRM Colombia, Perú y Chile", layout="wide")
st.title("Sistema contable multi-país")
st.markdown("Bienvenido, este es un sistema de prueba para Colombia, Perú y Chile.")
st.subheader("🔍 Probando conexión con Supabase...")

# --- Función para obtener los datos de la tabla ---
@st.cache_data(ttl=60)
def obtener_trm():
    try:
        response = supabase.table("trm_rates").select("*").order("id", desc=True).execute()
        data = response.data
        df = pd.DataFrame(data)

        # Convertir fechas (usamos formato 'mixed' para evitar errores)
        if "date_updated" in df.columns:
            df['date_updated'] = pd.to_datetime(df['date_updated'], format='mixed')
        return df
    except Exception as e:
        st.error("❌ Error al leer datos de Supabase.")
        st.exception(e)
        return pd.DataFrame()

# --- Mostrar tabla actual ---
df_trm = obtener_trm()
if not df_trm.empty:
    st.success("✅ Datos obtenidos con éxito")
    st.dataframe(df_trm)
else:
    st.warning("⚠️ No se encontraron registros o hubo un error.")

# --- Formulario para agregar nueva TRM ---
st.subheader("➕ Agregar nueva TRM")
with st.form("form_trm"):
    moneda = st.text_input("Moneda (ej: COP, PEN, CLP)").strip().upper()
    tasa = st.number_input("Tasa", min_value=0.0001, step=0.0001, format="%.4f")
    actualizado_por = st.text_input("Actualizado por", value="jordy_mora")
    submit = st.form_submit_button("Guardar")

    if submit:
        if moneda and tasa > 0:
            try:
                fecha_actual = datetime.utcnow().isoformat()
                response = supabase.table("trm_rates").insert({
                    "currency": moneda,
                    "rate": tasa,
                    "updated_by": actualizado_por,
                    "date_updated": fecha_actual
                }).execute()
                st.success("✅ TRM guardada correctamente")
                st.rerun()
            except Exception as e:
                st.error("❌ Error al guardar TRM.")
                st.exception(e)
        else:
            st.warning("⚠️ Por favor completa todos los campos.")
