import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client, Client

# 🔐 Claves de conexión a Supabase
url = "https://qzexuqkedukcwcyhrpza.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"

supabase: Client = create_client(url, key)

st.title("💱 Historial de TRM")
st.write("Consulta todas las tasas de cambio registradas por país.")

# 🔄 Leer registros
try:
    response = supabase.table("trm_rates").select("*").order("id", desc=True).execute()
    data = response.data
    df = pd.DataFrame(data)

    # Filtro por moneda
    monedas = ["Todas"] + sorted(df["currency"].str.upper().unique().tolist())
    moneda_seleccionada = st.selectbox("Filtrar por moneda:", monedas)

    if moneda_seleccionada != "Todas":
        df = df[df["currency"].str.upper() == moneda_seleccionada.upper()]

    st.dataframe(df)

except Exception as e:
    st.error("❌ Error al leer datos de Supabase.")
    st.exception(e)

# 🆕 Agregar nueva TRM
st.subheader("🆕 Agregar nueva TRM")
with st.form("agregar_trm"):
    nueva_moneda = st.text_input("Moneda (ej: COP, PEN, CLP)")
    nueva_tasa = st.number_input("Tasa", min_value=0.0, format="%.4f")
    usuario = st.text_input("Actualizado por", value="jordy_mora")
    submitted = st.form_submit_button("Guardar")

    if submitted:
        try:
            now = datetime.utcnow().isoformat()
            data = {
                "currency": nueva_moneda,
                "rate": nueva_tasa,
                "date_updated": now,
                "updated_by": usuario
            }
            insert_response = supabase.table("trm_rates").insert(data).execute()
            st.success("✅ Nueva TRM guardada con éxito. Recarga para ver el cambio.")
        except Exception as e:
            st.error("❌ Error inesperado al guardar TRM.")
            st.exception(e)

# ✏️ Editar TRM existente
st.subheader("✏️ Editar TRM existente")
try:
    if not df.empty:
        id_editar = st.selectbox("Selecciona el ID a editar", df["id"])
        trm_actual = df[df["id"] == id_editar].iloc[0]

        with st.form("editar_trm"):
            moneda_edit = st.text_input("Moneda", value=trm_actual["currency"])
            tasa_edit = st.number_input("Tasa", min_value=0.0, format="%.4f", value=float(trm_actual["rate"]))
            usuario_edit = st.text_input("Actualizado por", value="jordy_mora")
            submitted_edit = st.form_submit_button("Actualizar")

            if submitted_edit:
                try:
                    now = datetime.utcnow().isoformat()
                    update_response = supabase.table("trm_rates").update({
                        "currency": moneda_edit,
                        "rate": tasa_edit,
                        "date_updated": now,
                        "updated_by": usuario_edit
                    }).eq("id", id_editar).execute()
                    st.success("✅ TRM actualizada correctamente. Recarga para ver los cambios.")
                except Exception as e:
                    st.error("❌ Error al actualizar TRM.")
                    st.exception(e)
except Exception as e:
    st.error("❌ Error al preparar formulario de edición.")
    st.exception(e)
