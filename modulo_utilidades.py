"""
Módulo de Cálculo de Utilidades
Implementa todas las fórmulas del prompt según account_name
"""

import streamlit as st
import pandas as pd
import numpy as np
from supabase import create_client, Client
from datetime import datetime
import math
from typing import Dict, List, Optional, Tuple

class CalculadorUtilidades:
    """Clase principal para cálculo de utilidades según reglas de negocio"""
    
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.trm_actual = {}
        self.anexo_a = []
        self._cargar_datos_base()
    
    def _cargar_datos_base(self):
        """Carga TRM actual y tabla ANEXO A desde base de datos"""
        try:
            # Cargar TRM actual
            trm_result = self.supabase.table('trm_actual').select('*').execute()
            for trm in trm_result.data:
                self.trm_actual[trm['pais']] = float(trm['valor'])
            
            # Cargar ANEXO A
            anexo_result = self.supabase.table('anexo_a_pesos').select('*').eq('activo', True).execute()
            self.anexo_a = anexo_result.data
            
            st.success(f"✅ TRM cargadas: {list(self.trm_actual.keys())}")
            st.success(f"✅ ANEXO A cargado: {len(self.anexo_a)} rangos de peso")
            
        except Exception as e:
            st.error(f"❌ Error cargando datos base: {str(e)}")
            # Valores por defecto si falla la carga
            self.trm_actual = {'colombia': 4250.0, 'peru': 3.75, 'chile': 850.0}
            self.anexo_a = []
    
    def actualizar_trm(self, nuevas_trm: Dict[str, float], usuario: str = "sistema") -> bool:
        """Actualiza las TRM en base de datos y recalcula si es necesario"""
        try:
            cambios_significativos = []
            
            for pais, nuevo_valor in nuevas_trm.items():
                if pais in self.trm_actual:
                    valor_anterior = self.trm_actual[pais]
                    cambio_porcentual = ((nuevo_valor - valor_anterior) / valor_anterior) * 100
                    
                    # Actualizar en base de datos
                    self.supabase.table('trm_actual').update({
                        'valor': nuevo_valor,
                        'valor_anterior': valor_anterior,
                        'fecha_actualizacion': datetime.now().isoformat(),
                        'usuario_actualizacion': usuario
                    }).eq('pais', pais).execute()
                    
                    # Registrar en historial
                    self.supabase.table('trm_history').insert({
                        'pais': pais,
                        'valor_anterior': valor_anterior,
                        'valor_nuevo': nuevo_valor,
                        'cambio_porcentual': round(cambio_porcentual, 2),
                        'usuario': usuario,
                        'motivo': 'Actualización manual'
                    }).execute()
                    
                    # Actualizar valor local
                    self.trm_actual[pais] = nuevo_valor
                    
                    # Verificar si requiere recálculo
                    if abs(cambio_porcentual) > 1.0:  # Más del 1%
                        cambios_significativos.append(pais)
            
            # Mostrar resultado
            if cambios_significativos:
                st.warning(f"⚠️ Cambios significativos en TRM: {cambios_significativos}")
                st.info("💡 Se recomienda recalcular utilidades")
            
            return True
            
        except Exception as e:
            st.error(f"❌ Error actualizando TRM: {str(e)}")
            return False
    
    def buscar_gss_logistica(self, peso_kg: float) -> float:
        """Busca el valor de Gss Logística según el peso en la tabla ANEXO A"""
        for rango in self.anexo_a:
            if rango['peso_desde'] <= peso_kg <= rango['peso_hasta']:
                return float(rango['gss_logistica'])
        return 0.0
    
    def redondear_escala_05(self, valor: float) -> float:
        """Redondea a escala de 0.5 (1.2 -> 1.5, 1.8 -> 2.0)"""
        return math.ceil(valor * 2) / 2
    
    def limpiar_valores_monetarios(self, value) -> float:
        """Limpia valores monetarios formateados como strings"""
        if pd.isna(value) or value is None:
            return 0.0
        
        if isinstance(value, str):
            # Remover símbolos de moneda y comas
            clean_value = value.replace('$', '').replace(',', '').strip()
            try:
                return float(clean_value)
            except ValueError:
                return 0.0
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def calcular_utilidades_por_cuenta(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula utilidades según las reglas específicas de cada account_name
        Implementa exactamente el prompt proporcionado
        """
        st.info("🔄 Iniciando cálculo de utilidades por cuenta...")
        
        # Preparar DataFrame resultado con columnas base
        resultado_df = df[['Serial#', 'order_id', 'account_name', 'Asignacion']].copy()
        
        # Limpiar valores monetarios críticos
        for col in ['Declare Value', 'net_real_amount', 'logistics_total', 'aditionals_total', 'cxp_amt_due']:
            if col in df.columns:
                df[col] = df[col].apply(self.limpiar_valores_monetarios)
        
        # Inicializar todas las columnas posibles
        columnas_utilidad = [
            'Costo Amazon', 'Total & Adicional', 'MELI USD', 'Utilidad Gss',
            'Impuesto por facturación', 'Utilidad', 'Utilidad Socio',
            'Bodegal', 'Socio_cuenta', 'logistic_weight_ks', 'Gss Logística', 'Impuesto Gss'
        ]
        
        for col in columnas_utilidad:
            resultado_df[col] = np.nan
        
        # Procesar cada fila según su account_name
        for idx, row in df.iterrows():
            account_name = row.get('account_name', '')
            
            try:
                if account_name == '1-TODOENCARGO-CO':
                    self._calcular_todoencargo_co(resultado_df, idx, row)
                
                elif account_name == '4-MEGA TIENDAS PERUANAS':
                    self._calcular_mega_tiendas_peruanas(resultado_df, idx, row)
                
                elif account_name in ['5-DETODOPARATODOS', '6-COMPRAFACIL', '7-COMPRA-YA']:
                    self._calcular_detodoparatodos_group(resultado_df, idx, row)
                
                elif account_name in ['2-MEGATIENDA SPA', '3-VEENDELO']:
                    self._calcular_megatienda_spa_group(resultado_df, idx, row)
                
                elif account_name == '8-FABORCARGO':
                    self._calcular_faborcargo(resultado_df, idx, row)
                
                else:
                    # Account name no reconocido
                    resultado_df.loc[idx, 'Utilidad Gss'] = 0
                    st.warning(f"⚠️ Account name no reconocido: {account_name}")
            
            except Exception as e:
                st.error(f"❌ Error procesando fila {idx} ({account_name}): {str(e)}")
                continue
        
        # Estadísticas del procesamiento
        total_procesadas = len(resultado_df)
        utilidades_calculadas = resultado_df['Utilidad Gss'].notna().sum()
        utilidad_total = resultado_df['Utilidad Gss'].sum()
        
        st.success(f"✅ Procesamiento completado:")
        st.write(f"📊 Total órdenes: {total_procesadas}")
        st.write(f"🔢 Utilidades calculadas: {utilidades_calculadas}")
        st.write(f"💰 Utilidad total: ${utilidad_total:,.2f}")
        
        return resultado_df
    
    def _calcular_todoencargo_co(self, resultado_df: pd.DataFrame, idx: int, row: pd.Series):
        """Cálculo para 1-TODOENCARGO-CO"""
        declare_value = self.limpiar_valores_monetarios(row.get('Declare Value', 0))
        quantity = int(row.get('quantity', 1))
        net_real_amount = self.limpiar_valores_monetarios(row.get('net_real_amount', 0))
        logistics_total = self.limpiar_valores_monetarios(row.get('logistics_total', 0))
        aditionals_total = self.limpiar_valores_monetarios(row.get('aditionals_total', 0))
        
        # Cálculos según el prompt
        costo_amazon = declare_value * quantity
        total_adicional = logistics_total + aditionals_total
        meli_usd = net_real_amount / self.trm_actual.get('colombia', 4250.0)
        utilidad_gss = meli_usd - costo_amazon - total_adicional
        
        # Asignar valores
        resultado_df.loc[idx, 'Costo Amazon'] = costo_amazon
        resultado_df.loc[idx, 'Total & Adicional'] = total_adicional
        resultado_df.loc[idx, 'MELI USD'] = meli_usd
        resultado_df.loc[idx, 'Utilidad Gss'] = utilidad_gss
    
    def _calcular_mega_tiendas_peruanas(self, resultado_df: pd.DataFrame, idx: int, row: pd.Series):
        """Cálculo para 4-MEGA TIENDAS PERUANAS"""
        declare_value = self.limpiar_valores_monetarios(row.get('Declare Value', 0))
        quantity = int(row.get('quantity', 1))
        net_real_amount = self.limpiar_valores_monetarios(row.get('net_real_amount', 0))
        logistics_total = self.limpiar_valores_monetarios(row.get('logistics_total', 0))
        aditionals_total = self.limpiar_valores_monetarios(row.get('aditionals_total', 0))
        
        # Cálculos según el prompt
        costo_amazon = declare_value * quantity
        total_adicional = logistics_total + aditionals_total
        meli_usd = net_real_amount / self.trm_actual.get('peru', 3.75)
        utilidad_gss = meli_usd - costo_amazon - total_adicional
        
        # Asignar valores
        resultado_df.loc[idx, 'Costo Amazon'] = costo_amazon
        resultado_df.loc[idx, 'Total & Adicional'] = total_adicional
        resultado_df.loc[idx, 'MELI USD'] = meli_usd
        resultado_df.loc[idx, 'Utilidad Gss'] = utilidad_gss
    
    def _calcular_detodoparatodos_group(self, resultado_df: pd.DataFrame, idx: int, row: pd.Series):
        """Cálculo para 5-DETODOPARATODOS, 6-COMPRAFACIL, 7-COMPRA-YA"""
        declare_value = self.limpiar_valores_monetarios(row.get('Declare Value', 0))
        quantity = int(row.get('quantity', 1))
        net_real_amount = self.limpiar_valores_monetarios(row.get('net_real_amount', 0))
        logistics_total = self.limpiar_valores_monetarios(row.get('logistics_total', 0))
        aditionals_total = self.limpiar_valores_monetarios(row.get('aditionals_total', 0))
        order_status_meli = row.get('order_status_meli', '')
        
        # Cálculos según el prompt
        costo_amazon = declare_value * quantity
        total_adicional = logistics_total + aditionals_total
        meli_usd = net_real_amount / self.trm_actual.get('colombia', 4250.0)
        
        # Impuesto por facturación
        impuesto_facturacion = 1 if order_status_meli == 'approved' else 0
        
        # Utilidad base
        utilidad = meli_usd - costo_amazon - total_adicional - impuesto_facturacion
        
        # Distribución de utilidades según regla 7.5
        if utilidad >= 7.5:
            utilidad_socio = 7.5
            utilidad_gss = utilidad - 7.5
        else:
            utilidad_socio = utilidad
            utilidad_gss = 0
        
        # Asignar valores
        resultado_df.loc[idx, 'Costo Amazon'] = costo_amazon
        resultado_df.loc[idx, 'Total & Adicional'] = total_adicional
        resultado_df.loc[idx, 'MELI USD'] = meli_usd
        resultado_df.loc[idx, 'Impuesto por facturación'] = impuesto_facturacion
        resultado_df.loc[idx, 'Utilidad'] = utilidad
        resultado_df.loc[idx, 'Utilidad Socio'] = utilidad_socio
        resultado_df.loc[idx, 'Utilidad Gss'] = utilidad_gss
    
    def _calcular_megatienda_spa_group(self, resultado_df: pd.DataFrame, idx: int, row: pd.Series):
        """Cálculo para 2-MEGATIENDA SPA, 3-VEENDELO"""
        declare_value = self.limpiar_valores_monetarios(row.get('Declare Value', 0))
        quantity = int(row.get('quantity', 1))
        net_real_amount = self.limpiar_valores_monetarios(row.get('net_real_amount', 0))
        logistic_type = row.get('logistic_type', '')
        order_status_meli = row.get('order_status_meli', '')
        cxp_amt_due = self.limpiar_valores_monetarios(row.get('cxp_amt_due', 0))
        
        # Cálculos según el prompt
        costo_amazon = declare_value * quantity
        
        # Bodegal
        bodegal = 3.5 if logistic_type == 'xd_drop_off' else 0
        
        # Socio_cuenta
        socio_cuenta = 0 if order_status_meli == 'refunded' else 1
        
        # MELI USD
        meli_usd = net_real_amount / self.trm_actual.get('chile', 850.0)
        
        # Utilidad Gss
        utilidad_gss = meli_usd - cxp_amt_due - costo_amazon - bodegal - socio_cuenta
        
        # Asignar valores
        resultado_df.loc[idx, 'Costo Amazon'] = costo_amazon
        resultado_df.loc[idx, 'Bodegal'] = bodegal
        resultado_df.loc[idx, 'Socio_cuenta'] = socio_cuenta
        resultado_df.loc[idx, 'MELI USD'] = meli_usd
        resultado_df.loc[idx, 'Utilidad Gss'] = utilidad_gss
    
    def _calcular_faborcargo(self, resultado_df: pd.DataFrame, idx: int, row: pd.Series):
        """Cálculo para 8-FABORCARGO"""
        logistic_weight_lbs = self.limpiar_valores_monetarios(row.get('logistic_weight_lbs', 0))
        logistic_type = row.get('logistic_type', '')
        cxp_arancel = self.limpiar_valores_monetarios(row.get('cxp_arancel', 0))
        cxp_iva = self.limpiar_valores_monetarios(row.get('cxp_iva', 0))
        cxp_amt_due = self.limpiar_valores_monetarios(row.get('cxp_amt_due', 0))
        
        # Conversión de peso
        logistic_weight_ks = self.redondear_escala_05(logistic_weight_lbs / 2.20462)
        
        # Buscar Gss Logística en ANEXO A
        gss_logistica = self.buscar_gss_logistica(logistic_weight_ks)
        
        # Bodegal
        bodegal = 3.5 if logistic_type == 'xd_drop_off' else 0
        
        # Impuesto Gss
        impuesto_gss = cxp_arancel + cxp_iva
        
        # Utilidad Gss
        utilidad_gss = gss_logistica + impuesto_gss - cxp_amt_due
        
        # Asignar valores
        resultado_df.loc[idx, 'logistic_weight_ks'] = logistic_weight_ks
        resultado_df.loc[idx, 'Gss Logística'] = gss_logistica
        resultado_df.loc[idx, 'Bodegal'] = bodegal
        resultado_df.loc[idx, 'Impuesto Gss'] = impuesto_gss
        resultado_df.loc[idx, 'Utilidad Gss'] = utilidad_gss
    
    def guardar_utilidades_en_bd(self, df_utilidades: pd.DataFrame, usuario: str = "sistema") -> bool:
        """Guarda los resultados de utilidades en la base de datos"""
        try:
            st.info("💾 Guardando utilidades en base de datos...")
            
            # Preparar registros para inserción
            registros = []
            
            for idx, row in df_utilidades.iterrows():
                registro = {
                    'serial_number': str(row.get('Serial#', '')),
                    'order_id': str(row.get('order_id', '')),
                    'account_name': str(row.get('account_name', '')),
                    'asignacion': str(row.get('Asignacion', '')),
                    'costo_amazon': float(row.get('Costo Amazon', 0)) if pd.notna(row.get('Costo Amazon')) else None,
                    'meli_usd': float(row.get('MELI USD', 0)) if pd.notna(row.get('MELI USD')) else None,
                    'utilidad_gss': float(row.get('Utilidad Gss', 0)) if pd.notna(row.get('Utilidad Gss')) else None,
                    'utilidad_socio': float(row.get('Utilidad Socio', 0)) if pd.notna(row.get('Utilidad Socio')) else None,
                    'total_adicional': float(row.get('Total & Adicional', 0)) if pd.notna(row.get('Total & Adicional')) else None,
                    'trm_colombia': self.trm_actual.get('colombia'),
                    'trm_peru': self.trm_actual.get('peru'),
                    'trm_chile': self.trm_actual.get('chile'),
                    'usuario_calculo': usuario,
                    'fecha_calculo': datetime.now().isoformat()
                }
                
                # Limpiar valores None
                registro = {k: v for k, v in registro.items() if v is not None}
                registros.append(registro)
            
            # Insertar en lotes
            batch_size = 50
            total_insertados = 0
            
            for i in range(0, len(registros), batch_size):
                batch = registros[i:i + batch_size]
                result = self.supabase.table('utilidades_calculadas').insert(batch).execute()
                total_insertados += len(batch)
                
                # Mostrar progreso
                progreso = min(100, (i + batch_size) / len(registros) * 100)
                st.progress(progreso / 100)
            
            st.success(f"✅ {total_insertados} registros de utilidades guardados correctamente")
            return True
            
        except Exception as e:
            st.error(f"❌ Error guardando utilidades: {str(e)}")
            return False
    
    def obtener_estadisticas_cuenta(self, account_name: str = None) -> pd.DataFrame:
        """Obtiene estadísticas de utilidades por cuenta"""
        try:
            if account_name:
                result = self.supabase.table('estadisticas_utilidades').select('*').eq('account_name', account_name).execute()
            else:
                result = self.supabase.table('estadisticas_utilidades').select('*').execute()
            
            return pd.DataFrame(result.data)
            
        except Exception as e:
            st.error(f"❌ Error obteniendo estadísticas: {str(e)}")
            return pd.DataFrame()
    
    def obtener_historial_trm(self, pais: str = None, dias: int = 30) -> pd.DataFrame:
        """Obtiene historial de cambios TRM"""
        try:
            fecha_limite = (datetime.now() - pd.Timedelta(days=dias)).isoformat()
            
            query = self.supabase.table('trm_history').select('*').gte('fecha_cambio', fecha_limite).order('fecha_cambio', desc=True)
            
            if pais:
                query = query.eq('pais', pais)
            
            result = query.execute()
            return pd.DataFrame(result.data)
            
        except Exception as e:
            st.error(f"❌ Error obteniendo historial TRM: {str(e)}")
            return pd.DataFrame()

# Función de utilidad para usar en Streamlit - CREDENCIALES DIRECTAS
@st.cache_resource
def get_calculador_utilidades():
    """Factory function para obtener instancia del calculador con cache"""
    # USAR CREDENCIALES DIRECTAS (igual que en streamlit_app.py)
    supabase_url = "https://qzexuqkedukcwcyhrpza.supabase.co"
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6ZXh1cWtlZHVrY3djeWhycHphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3NDEzODcsImV4cCI6MjA2OTMxNzM4N30.T_lXTVGZCFGA5rjVWQNo3WphIE2YPaifxonHIGPMkI0"
    
    supabase = create_client(supabase_url, supabase_key)
    
    return CalculadorUtilidades(supabase)
