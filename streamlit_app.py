import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime

# 🔐 Claves de conexión a Supabase
url = "https://qzexuqkedukcwcyhrpza.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"

# 🤖 Crear cliente de Supabase
supabase: Client = create_client(url, key)

# 🖥️ Interfaz
st.set_page_config(page_title="Sistema contable multi-país", layout="centered")
st.title("Sistema contable multi-país")
st.write("Bienvenido, este es un sistema de prueba para Colombia, Perú y Chile.")

st.subheader("🔍 Probando conexión con Supabase...")

# 📊 Leer datos de TRM
try:
    data = supabase.table("trm_rates").select("*").order("id", desc=True).limit(100).execute()
    df = pd.DataFrame(data.data)
    if not df.empty:
        df["created_at"] = pd.to_datetime(df.get("created_at", datetime.now()))
        st.success("✅ Datos de TRM obtenidos con éxito")
        st.dataframe(df)
    else:
        st.warning("⚠️ No hay datos en la tabla trm_rates")

except Exception as e:
    st.error("❌ Error al leer datos de Supabase.")
    st.exception(e)

# 🆕 Formulario para agregar o editar TRM
st.subheader("🛠️ Agregar o Editar TRM")

with st.form("form_trm"):
    moneda = st.text_input("Moneda (ej: COP, PEN, CLP)").strip().upper()
    tasa = st.number_input("Tasa", min_value=0.0001, format="%.4f")
    actualizado_por = st.text_input("Actualizado por", value="jordy_mora")

    modo_edicion = st.checkbox("Editar si ya existe")

    submitted = st.form_submit_button("💾 Guardar TRM")

    if submitted:
        if moneda and tasa > 0 and actualizado_por:
            try:
                if modo_edicion:
                    # Buscar si ya existe y actualizar
                    existing = supabase.table("trm_rates").select("*").eq("currency", moneda).execute()
                    if existing.data:
                        row_id = existing.data[0]["id"]
                        response = supabase.table("trm_rates").update({
                            "rate": tasa,
                            "updated_by": actualizado_por,
                            "date_updated": datetime.now().isoformat()
                        }).eq("id", row_id).execute()
                        st.success("✅ TRM actualizada correctamente")
                    else:
                        st.warning("⚠️ No existe TRM para esa moneda, se insertará nueva.")
                        response = supabase.table("trm_rates").insert({
                            "currency": moneda,
                            "rate": tasa,
                            "updated_by": actualizado_por,
                            "date_updated": datetime.now().isoformat()
                        }).execute()
                        st.success("✅ TRM insertada correctamente")

                else:
                    response = supabase.table("trm_rates").insert({
                        "currency": moneda,
                        "rate": tasa,
                        "updated_by": actualizado_por,
                        "date_updated": datetime.now().isoformat()
                    }).execute()
                    st.success("✅ TRM insertada correctamente")

            except Exception as e:
                st.error("❌ Error inesperado al guardar TRM.")
                st.exception(e)
        else:
            st.warning("⚠️ Todos los campos son obligatorios.")
