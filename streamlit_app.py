import streamlit as st
import pandas as pd
from supabase import create_client, Client

# 🔐 Claves Supabase
url = "https://qzexuqkedukcwcyhrpza.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"
supabase: Client = create_client(url, key)

# 🧾 Título
st.title("💱 Historial de TRM")
st.write("Consulta todas las tasas de cambio registradas por país.")

try:
    # 🚀 Obtener datos sin ordenar aún
    response = supabase.table("trm_rates").select("*").execute()
    data = response.data

    if not data:
        st.warning("⚠️ No hay registros en la tabla `trm_rates`.")
    else:
        df = pd.DataFrame(data)

        # 📅 Convertir y ordenar por fecha
        df["created_at"] = pd.to_datetime(df["created_at"])
        df = df.sort_values(by="created_at", ascending=False)

        # 🔎 Filtro por moneda
        monedas = ["Todas"] + sorted(df["currency"].dropna().unique())
        moneda = st.selectbox("Filtrar por moneda:", monedas)

        if moneda != "Todas":
            df = df[df["currency"] == moneda]

        # 📆 Mostrar tabla
        df["created_at"] = df["created_at"].dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(df)

except Exception as e:
    st.error("❌ Error al leer datos de Supabase.")
    st.exception(e)
