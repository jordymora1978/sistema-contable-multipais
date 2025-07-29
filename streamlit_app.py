import streamlit as st
st.title("Test")
st.write("Funciona")
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
import time
import re
st.set_page_config(
    page_title="Sistema Contable Multi-País",
    page_icon="🌎",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def init_supabase():
    try:
        url = st.secrets.get("supabase", {}).get("url", "https://qzexuqkedukcwcyhrpza.supabase.co")
        key = st.secrets.get("supabase", {}).get("key", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0")
        supabase: Client = create_client(url, key)
        return supabase
    except Exception as e:
        st.error(f"Error conectando a Supabase: {str(e)}")
        return None
