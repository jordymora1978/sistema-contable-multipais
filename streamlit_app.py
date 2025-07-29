import streamlit as st
import pandas as pd
from supabase import create_client, Client

# 🔐 Conexión Supabase
url = "https://qzexuqkedukcwcyhrpza.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"
supabase: Client = create_client(url, key)

# 🧾 Título principal
st.title("Sistema contable multi-país")
st.markdown("Bienvenido, este es un sistema de prueba para Colombia, Perú y Chile.")

# 🧠 Leer todas las TRM desde Supabase
try:
    response = supabase.table("trm_rates").select("*").order("created_at", desc=True).execute()
    trm_data = pd.DataFrame(response.data)

    if not trm_data.empty:
        st.success("✅ Datos cargados correctamente")

        # 🔎 Filtros
        monedas = ["Todas"] + sorted(trm_data["currency"].unique().tolist())
        moneda_filtrada = st.selectbox("Filtrar por moneda", monedas)

        if moneda_filtrada != "Todas":
            trm_data = trm_data[trm_data["currency"] == moneda_filtrada]

        # 📅 Formatear fecha
        trm_data["created_at"] = pd.to_datetime(trm_data["created_at"]).dt.strftime("%Y-%m-%d %H:%M")

        # 🧾 Mostrar tabla
        st.dataframe(trm_data)

    else:
        st.info("No hay TRMs registradas aún.")

except Exception as e:
    st.error("❌ Error al leer datos de trm_rates")
    st.exception(e)
