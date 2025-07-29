import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client, Client

# 🔐 Claves de conexión a Supabase
url = "https://qzexuqkedukcwcyhrpza.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"

# 🤖 Crear cliente Supabase
supabase: Client = create_client(url, key)

# 🖥️ Título
st.title("Sistema contable multi-país")
st.write("Bienvenido, este es un sistema de prueba para Colombia, Perú y Chile.")
st.subheader("🔍 Probando conexión con Supabase...")

# 📥 Formulario para ingresar nueva TRM
st.subheader("🆕 Agregar nueva TRM")
with st.form("form_trm"):
    currency = st.text_input("Moneda (ej: COP, PEN, CLP)").upper()
    rate = st.number_input("Tasa", min_value=0.0, format="%.4f")
    updated_by = st.text_input("Actualizado por", value="jordy_mora")
    submitted = st.form_submit_button("Guardar")

    if submitted:
        # ✅ Validación
        if not currency or len(currency) != 3:
            st.warning("⚠️ Ingresa un código de moneda válido de 3 letras (ej: COP).")
        elif rate <= 0:
            st.warning("⚠️ La tasa debe ser mayor que cero.")
        elif not updated_by.strip():
            st.warning("⚠️ El campo 'Actualizado por' no puede estar vacío.")
        else:
            try:
                now = datetime.utcnow().isoformat()
                response = supabase.table("trm_rates").insert({
                    "currency": currency,
                    "rate": rate,
                    "date_updated": now,
                    "updated_by": updated_by
                }).execute()

                if response.status_code == 201:
                    st.success("✅ Nueva TRM guardada exitosamente.")
                else:
                    st.error("❌ Error al guardar en Supabase.")
                    st.json(response.json())
            except Exception as e:
                st.error("❌ Error inesperado al guardar TRM.")
                st.exception(e)

# 📊 Mostrar últimos registros
st.subheader("📄 Últimos registros de TRM")
try:
    data = supabase.table("trm_rates").select("*").limit(10).order("id", desc=True).execute()
    df = pd.DataFrame(data.data)
    st.dataframe(df)
except Exception as e:
    st.error("❌ Error al leer datos de trm_rates")
    st.exception(e)
