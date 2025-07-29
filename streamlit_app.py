import streamlit as st
from supabase import create_client, Client
import pandas as pd

# 🔐 Claves de conexión a Supabase (no cambies esta URL si ya te funciona)
url = "https://qzexuqkedukcwchrpza.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"

# 🔌 Crear cliente de Supabase
supabase: Client = create_client(url, key)

# 🖼️ Interfaz de la app
st.title("Sistema contable multi-país")
st.write("Bienvenido, este es un sistema de prueba para Colombia, Perú y Chile.")
st.subheader("🔍 Probando conexión con Supabase...")

# 🧾 Formulario para nueva tasa
st.markdown("### Agregar nueva TRM")
currency = st.text_input("Moneda (ej: COP, PEN, CLP)")
rate = st.number_input("Tasa", min_value=0.0, format="%.4f")
updated_by = st.text_input("Actualizado por", value="jordy_mora")

# 📥 Mostrar datos actuales de la tabla
try:
    data = supabase.table("trm_rates").select("*").limit(10).order("id", desc=True).execute()
    st.success("✅ Datos de TRM obtenidos con éxito")
    df = pd.DataFrame(data.data)
    st.dataframe(df)
except Exception as e:
    st.error("❌ Error al leer datos de trm_rates")
    st.exception(e)
