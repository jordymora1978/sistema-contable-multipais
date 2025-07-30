import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- Configuración de Supabase ---
# NOTA DE SEGURIDAD: Hardcodear las claves API directamente en el código
# no es la mejor práctica para aplicaciones en producción o repositorios públicos.
# Se recomienda encarecidamente usar Streamlit Secrets (como se muestra en los comentarios)
# o variables de entorno para una gestión segura de las credenciales.

# Reemplaza 'TU_URL_SUPABASE' con la URL de tu proyecto Supabase
# y 'TU_ANON_KEY_SUPABASE' con tu clave anon public.
# Para tu proyecto 'pqlvmxnhztajoxkwyuoh', la URL sería algo como:
# https://pqlvmxnhztajoxkwyuoh.supabase.co
# La clave que proporcionaste es: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBxbHZteG5oenRham94a3d5dW9oIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM4ODgzOTAsImV4cCI6MjA2OTQ2NDM5MH0.ITpUjsOt9yYVe5AFWetsZHn_3RpH05uV8xmyq99nqfs

# Usando la clave proporcionada directamente (menos seguro para producción)
supabase_url: str = "https://pqlvmxnhztajoxkwyuoh.supabase.co" # Ajusta si tu URL es diferente
supabase_key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBxbHZteG5oenRham94a3d5dW9oIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM4ODgzOTAsImV4cCI6MjA2OTQ2NDM5MH0.ITpUjsOt9yYVe5AFWetsZHn_3RpH05uV8xmyq99nqfs"

try:
    supabase: Client = create_client(supabase_url, supabase_key)
except Exception as e:
    st.error(f"Error al inicializar el cliente de Supabase: {e}")
    st.stop()

# Nombre de la tabla en Supabase
TABLE_NAME = "drapify_consolidated_data"

st.set_page_config(layout="wide", page_title="Drapify Data Uploader")

st.title("📊 Drapify Data Uploader a Supabase")

st.markdown("""
Esta aplicación te permite subir tu archivo CSV consolidado a una base de datos de Supabase.
Asegúrate de que tu archivo CSV esté correctamente formateado.
""")

# --- Carga de Archivo CSV ---
uploaded_file = st.file_uploader("Sube tu archivo CSV consolidado aquí", type=["csv"])

if uploaded_file is not None:
    st.success("Archivo CSV cargado exitosamente.")
    
    # Leer el archivo CSV en un DataFrame de pandas
    try:
        df = pd.read_csv(uploaded_file)
        st.write("Vista previa de los datos cargados:")
        st.dataframe(df.head())

        st.write(f"Columnas en el archivo CSV: {df.columns.tolist()}")

        # Botón para subir a Supabase
        if st.button(f"Subir datos a la tabla '{TABLE_NAME}' en Supabase"):
            st.info("Subiendo datos a Supabase... Esto puede tardar un momento.")
            
            # Limpiar nombres de columnas para Supabase (convertir a minúsculas, reemplazar espacios, etc.)
            df.columns = [col.lower().replace('#', 'num').replace(' ', '_').replace('.', '').replace('/', '_').replace('-', '_').replace('(', '').replace(')', '').replace(':', '').replace('__', '_') for col in df.columns]

            # Convertir todos los valores de NaN a None para que Supabase los maneje correctamente
            df = df.where(pd.notna(df), None)

            # Convertir DataFrame a una lista de diccionarios
            data_to_insert = df.to_dict(orient='records')

            # Insertar datos en Supabase
            try:
                # Primero, intentar eliminar la tabla si existe para evitar duplicados en cada subida
                # Esto es opcional y puede ser riesgoso en producción si no se maneja con cuidado.
                # Para este ejemplo, asumiremos que queremos sobrescribir o que la tabla está vacía.
                # Supabase no tiene un método directo para "truncate" desde el cliente,
                # así que para este ejemplo, si necesitas sobrescribir, tendrías que eliminar y recrear la tabla
                # manualmente en la interfaz de Supabase, o implementar lógica de upsert/delete por IDs.
                # Por simplicidad, aquí solo insertaremos. Si subes el mismo archivo varias veces,
                # tendrás duplicados a menos que borres la tabla en Supabase.

                # Insertar los datos
                # Supabase tiene un límite de tamaño de fila. Para archivos grandes, es mejor insertar en lotes.
                batch_size = 1000
                total_rows = len(data_to_insert)
                
                with st.spinner(f"Subiendo {total_rows} filas..."):
                    for i in range(0, total_rows, batch_size):
                        batch = data_to_insert[i:i + batch_size]
                        response = supabase.table(TABLE_NAME).insert(batch).execute()
                        
                        # Verificar si la inserción fue exitosa
                        if response.data:
                            st.write(f"✅ Lote {i // batch_size + 1} de {len(batch)} filas subido exitosamente.")
                        else:
                            st.error(f"❌ Error al subir el lote {i // batch_size + 1}: {response.error}")
                            break # Detener si hay un error en un lote

                st.success("¡Datos subidos a Supabase exitosamente!")
                st.balloons()

            except Exception as e:
                st.error(f"Error al subir datos a Supabase: {e}")
    except Exception as e:
        st.error(f"Error al leer el archivo CSV. Asegúrate de que esté bien formateado: {e}")

st.markdown("---")
st.header("🔍 Ver datos en Supabase")

# Botón para cargar y mostrar datos desde Supabase
if st.button(f"Cargar y mostrar datos de '{TABLE_NAME}' desde Supabase"):
    st.info("Cargando datos desde Supabase...")
    try:
        response = supabase.table(TABLE_NAME).select("*").limit(100).execute() # Limitar a 100 filas para vista previa
        if response.data:
            df_from_supabase = pd.DataFrame(response.data)
            st.subheader(f"Primeras 100 filas de la tabla '{TABLE_NAME}' en Supabase:")
            st.dataframe(df_from_supabase)
            st.success("Datos cargados desde Supabase exitosamente.")
        else:
            st.warning(f"No se encontraron datos en la tabla '{TABLE_NAME}'.")
    except Exception as e:
        st.error(f"Error al cargar datos desde Supabase: {e}")

st.markdown("""
**Instrucciones adicionales:**
1.  **Crea tu proyecto Supabase:** Ve a [Supabase](https://supabase.com/) y crea un nuevo proyecto.
2.  **Crea una tabla:** En tu proyecto Supabase, ve a "Table Editor" y crea una nueva tabla llamada `drapify_consolidated_data`. Puedes empezar con una tabla vacía, ya que esta aplicación insertará los datos. Supabase inferirá los tipos de datos.
3.  **Obtén tus credenciales:** Ve a "Project Settings" -> "API" en Supabase. Necesitarás tu `URL` y tu `anon public key`.
4.  **Configura Streamlit Secrets (RECOMENDADO PARA PRODUCCIÓN):**
    * Si estás usando Streamlit Cloud, ve a tu repositorio de GitHub, luego a "Settings" -> "Secrets". Agrega los secretos `supabase_url` y `supabase_key` bajo la sección `[supabase]`.
    * Si estás ejecutando localmente, crea una carpeta `.streamlit` en la raíz de tu proyecto y dentro de ella un archivo `secrets.toml`. Pega tus credenciales allí como se indica al inicio del código.
5.  **Despliega en Streamlit Cloud:** Sube este archivo `app.py` a un repositorio de GitHub. Luego, ve a [Streamlit Cloud](https://share.streamlit.io/) y despliega tu aplicación desde ese repositorio.
""")
