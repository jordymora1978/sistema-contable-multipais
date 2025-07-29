import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client, Client

# 🌐 Conexión a Supabase
url = "https://qzexuqkedukcwcyhrpza.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"
supabase: Client = create_client(url, key)

# 🖥️ Interfaz Streamlit
st.title("Sistema contable multi-país")
st.write("Bienvenido, este es un sistema de prueba para Colombia, Perú y Chile.")

st.subheader("🔍 Probando conexión con Supabase...")

# 📊 Leer datos de TRM
try:
    data = supabase.table("trm_rates").select("*").limit(10).order("id", desc=True).execute()
    df = pd.DataFrame(data.data)
    st.success("✅ Datos de TRM obtenidos con éxito")
    st.dataframe(df)
except Exception as e:
    st.error("❌ Error al leer datos de trm_rates")
    st.exception(e)

# ➕ Formulario para nueva TRM
st.subheader("Agregar nueva TRM")

with st.form("trm_form"):
    moneda = st.text_input("Moneda (ej: COP, PEN, CLP)")
    tasa = st.number_input("Tasa", format="%.4f")
    actualizado_por = st.text_input("Actualizado por", value="jordy_mora")
    submitted = st.form_submit_button("Guardar TRM")

    if submitted:
        try:
            hoy = datetime.utcnow().date().isoformat()

            # Buscar si ya existe TRM hoy para esta moneda
            existente = supabase.table("trm_rates").select("*").eq("currency", moneda).eq("date_updated", hoy).execute()

            if existente.data:
                st.warning(f"⚠️ Ya existe una TRM para {moneda} en {hoy}. No se guardó.")
            else:
                nueva = {
                    "currency": moneda,
                    "rate": tasa,
                    "updated_by": actualizado_por,
                    "date_updated": hoy
                }
                result = supabase.table("trm_rates").insert(nueva).execute()
                st.success("✅ TRM guardada correctamente.")

        except Exception as e:
            st.error("❌ Error inesperado al guardar TRM.")
            st.exception(e)
