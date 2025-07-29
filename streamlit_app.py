import streamlit as st
import pandas as pd
from supabase import create_client, Client

# 🔐 Conexión Supabase
url = "https://qzexuqkedukcwcyhrpza.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"
supabase: Client = create_client(url, key)

# 🧾 Título
st.title("💱 Historial de TRM")
st.write("Consulta todas las tasas de cambio registradas por país.")

try:
    # 🚀 Obtener datos
    response = supabase.table("trm_rates").select("*").execute()
    data = response.data

    if not data:
        st.warning("⚠️ No hay registros en la tabla `trm_rates`.")
    else:
        df = pd.DataFrame(data)

        # ✅ Ordenar si existe 'id'
        if "id" in df.columns:
            df = df.sort_values(by="id", ascending=False)

        # 🔎 Filtro por moneda
        if "currency" in df.columns:
            monedas = ["Todas"] + sorted(df["currency"].dropna().unique())
            moneda = st.selectbox("Filtrar por moneda:", monedas)
            if moneda != "Todas":
                df = df[df["currency"] == moneda]

        # 📆 Si existe 'created_at', formatear
        if "created_at" in df.columns:
            df["created_at"] = pd.to_datetime(df["created_at"]).dt.strftime("%Y-%m-%d %H:%M")

        # Mostrar resultados
        st.dataframe(df)

except Exception as e:
    st.error("❌ Error al leer datos de Supabase.")
    st.exception(e)
