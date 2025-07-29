import streamlit as st
from supabase import create_client, Client

# 🔐 Claves de conexión a Supabase
url = "https://qzexuqkedukcwcyhrpza.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"  # 👈 Reemplaza esta línea con tu clave real copiada desde Supabase

# 🤖 Crear cliente de Supabase
supabase: Client = create_client(url, key)

# 🖥️ Interfaz básica de la app
st.title("Sistema contable multi-país")
st.write("Bienvenido, este es un sistema de prueba para Colombia, Perú y Chile.")
st.subheader("🔍 Probando conexión con Supabase...")

# 📊 Intentar leer datos de la tabla trm_rates
try:
    data = supabase.table("trm_rates").select("*").limit(10).execute()
    st.success("✅ Datos de TRM obtenidos con éxito")
    st.json(data.data)
except Exception as e:
    st.error("❌ Error al leer datos de trm_rates")
    st.exception(e)
