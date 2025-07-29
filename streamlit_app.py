import streamlit as st
from supabase import create_client, Client
from datetime import datetime

# 🔑 Claves de conexión a Supabase
url = "https://qzexuqkedukcwchrpza.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"

# 🧠 Crear cliente Supabase
supabase: Client = create_client(url, key)

# 🖥️ Interfaz
st.title("Sistema contable multi-país")
st.write("Bienvenido, este es un sistema de prueba para Colombia, Perú y Chile.")
st.subheader("🔍 Probando conexión con Supabase...")

# 📥 Formulario para insertar nueva TRM
with st.form("form_insert"):
    st.write("### Agregar nueva TRM")
    currency = st.text_input("Moneda (ej: COP, PEN, CLP)")
    rate = st.number_input("Tasa", min_value=0.0, format="%.4f")
    updated_by = st.text_input("Actualizado por", value="jordy_mora")

    submitted = st.form_submit_button("Guardar en Supabase")

    if submitted:
        try:
            response = supabase.table("trm_rates").insert({
                "currency": currency,
                "rate": rate,
                "date_updated": datetime.utcnow().isoformat(),
                "updated_by": updated_by
            }).execute()
            st.success("✅ TRM guardada con éxito")
        except Exception as e:
            st.error("❌ Error al guardar en Supabase")
            st.exception(e)

# 📤 Mostrar últimos registros
try:
    data = supabase.table("trm_rates").select("*").limit(10).order("id", desc=True).execute()
    st.success("✅ Datos de TRM obtenidos con éxito")
    st.dataframe(data.data)
except Exception as e:
    st.error("❌ Error al leer datos de trm_rates")
    st.exception(e)
