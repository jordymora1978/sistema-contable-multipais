# SOLUCIÓN FLEXIBLE COMPLETA - REEMPLAZO PARA TU CÓDIGO
# Copia y pega estas funciones en tu archivo principal

import re
import pandas as pd
import numpy as np
from difflib import SequenceMatcher

def normalize_column_name(col_name):
    """
    Normaliza nombres de columnas para matching flexible
    Convierte cualquier variación a formato estándar para comparación
    """
    if pd.isna(col_name) or not col_name:
        return ""
    
    # Convertir a string y limpiar espacios
    normalized = str(col_name).strip()
    
    # Convertir a minúsculas
    normalized = normalized.lower()
    
    # Reemplazar espacios, puntos, # y otros caracteres especiales con guión bajo
    normalized = re.sub(r'[^\w]', '_', normalized)
    
    # Remover múltiples guiones bajos consecutivos
    normalized = re.sub(r'_+', '_', normalized)
    
    # Remover guiones bajos al inicio y final
    normalized = normalized.strip('_')
    
    return normalized

# DICCIONARIO MAESTRO DE MAPEO FLEXIBLE
FLEXIBLE_COLUMN_MAPPING = {
    # === COLUMNAS DRAPIFY ===
    'system_': 'System#',
    'system_number': 'System#',
    'system_num': 'System#',
    'systemnumber': 'System#',
    
    'serial_': 'Serial#',
    'serial_number': 'Serial#',
    'serial_num': 'Serial#',
    'serialnumber': 'Serial#',
    
    'order_id': 'order_id',
    'orderid': 'order_id',
    'order_number': 'order_id',
    'numero_orden': 'order_id',
    
    'pack_id': 'pack_id',
    'packid': 'pack_id',
    'package_id': 'pack_id',
    
    'asin': 'ASIN',
    
    'client_first_name': 'client_first_name',
    'clientfirstname': 'client_first_name',
    'first_name': 'client_first_name',
    'nombre': 'client_first_name',
    
    'client_last_name': 'client_last_name',
    'clientlastname': 'client_last_name',
    'last_name': 'client_last_name',
    'apellido': 'client_last_name',
    
    'client_doc_id': 'client_doc_id',
    'clientdocid': 'client_doc_id',
    'document_id': 'client_doc_id',
    'cedula': 'client_doc_id',
    
    'account_name': 'account_name',
    'accountname': 'account_name',
    'cuenta': 'account_name',
    
    'date_created': 'date_created',
    'datecreated': 'date_created',
    'fecha_creacion': 'date_created',
    'created_date': 'date_created',
    
    'quantity': 'quantity',
    'cantidad': 'quantity',
    'qty': 'quantity',
    
    'title': 'title',
    'titulo': 'title',
    'product_title': 'title',
    'nombre_producto': 'title',
    
    'unit_price': 'unit_price',
    'unitprice': 'unit_price',
    'precio_unitario': 'unit_price',
    'precio_unit': 'unit_price',
    
    'logistic_type': 'logistic_type',
    'logistictype': 'logistic_type',
    'tipo_logistica': 'logistic_type',
    
    'prealert_id': 'prealert_id',
    'prealertid': 'prealert_id',
    'pre_alert_id': 'prealert_id',
    'id_prealerta': 'prealert_id',
    
    'meli_fee': 'Meli Fee',
    'melifee': 'Meli Fee',
    'fee_meli': 'Meli Fee',
    'comision_meli': 'Meli Fee',
    
    'declare_value': 'Declare Value',
    'declarevalue': 'Declare Value',
    'valor_declarado': 'Declare Value',
    
    'iva': 'IVA',
    'i_v_a': 'IVA',
    'impuesto': 'IVA',
    'tax': 'IVA',
    
    'ica': 'ICA',
    'i_c_a': 'ICA',
    
    'fuente': 'FUENTE',
    'retencion_fuente': 'FUENTE',
    'ret_fuente': 'FUENTE',
    
    'senders_cost': 'senders_cost',
    'senderscost': 'senders_cost',
    'costo_remitente': 'senders_cost',
    
    'gross_amount': 'gross_amount',
    'grossamount': 'gross_amount',
    'monto_bruto': 'gross_amount',
    
    'net_received_amount': 'net_received_amount',
    'netreceivedamount': 'net_received_amount',
    'monto_neto_recibido': 'net_received_amount',
    
    'net_real_amount': 'net_real_amount',
    'netrealamount': 'net_real_amount',
    'monto_real_neto': 'net_real_amount',
    
    'logistic_weight_lbs': 'logistic_weight_lbs',
    'logisticweightlbs': 'logistic_weight_lbs',
    'peso_logistico_lbs': 'logistic_weight_lbs',
    
    'refunded_date': 'refunded_date',
    'refundeddate': 'refunded_date',
    'fecha_reembolso': 'refunded_date',
    
    # === COLUMNAS LOGISTICS ===
    'guide_number': 'Guide Number',
    'guidenumber': 'Guide Number',
    'numero_guia': 'Guide Number',
    'guia': 'Guide Number',
    
    'order_number': 'Order number',
    'ordernumber': 'Order number',
    'numero_orden': 'Order number',
    'order_num': 'Order number',
    
    'reference': 'Reference',
    'referencia': 'Reference',
    'ref': 'Reference',
    
    'sap_code': 'SAP Code',
    'sapcode': 'SAP Code',
    'codigo_sap': 'SAP Code',
    
    'invoice': 'Invoice',
    'factura': 'Invoice',
    'numero_factura': 'Invoice',
    
    'status': 'Status',
    'estado': 'Status',
    'estatus': 'Status',
    
    'fob': 'FOB',
    'f_o_b': 'FOB',
    
    'unit': 'Unit',
    'unidad': 'Unit',
    'und': 'Unit',
    
    'weight': 'Weight',
    'peso': 'Weight',
    'weight_kg': 'Weight',
    
    'length': 'Length',
    'largo': 'Length',
    'longitud': 'Length',
    
    'width': 'Width',
    'ancho': 'Width',
    'anchura': 'Width',
    
    'height': 'Height',
    'alto': 'Height',
    'altura': 'Height',
    
    'insurance': 'Insurance',
    'seguro': 'Insurance',
    'aseguranza': 'Insurance',
    
    'logistics': 'Logistics',
    'logistica': 'Logistics',
    'costo_logistico': 'Logistics',
    
    'duties_prealert': 'Duties Prealert',
    'dutiesprealert': 'Duties Prealert',
    'aranceles_prealerta': 'Duties Prealert',
    
    'duties_pay': 'Duties Pay',
    'dutiespay': 'Duties Pay',
    'pago_aranceles': 'Duties Pay',
    
    'duty_fee': 'Duty Fee',
    'dutyfee': 'Duty Fee',
    'tasa_arancel': 'Duty Fee',
    
    'saving': 'Saving',
    'ahorro': 'Saving',
    'descuento': 'Saving',
    
    'total': 'Total',
    'total_amount': 'Total',
    'monto_total': 'Total',
    
    'description': 'Description',
    'descripcion': 'Description',
    'desc': 'Description',
    
    'shipper': 'Shipper',
    'remitente': 'Shipper',
    'enviador': 'Shipper',
    
    'phone': 'Phone',
    'telefono': 'Phone',
    'celular': 'Phone',
    'tel': 'Phone',
    
    'consignee': 'Consignee',
    'consignatario': 'Consignee',
    'destinatario': 'Consignee',
    
    'identification': 'Identification',
    'identificacion': 'Identification',
    'cedula': 'Identification',
    'id': 'Identification',
    
    'country': 'Country',
    'pais': 'Country',
    'nacion': 'Country',
    
    'state': 'State',
    'estado': 'State',
    'provincia': 'State',
    'departamento': 'State',
    
    'city': 'City',
    'ciudad': 'City',
    'municipio': 'City',
    
    'address': 'Address',
    'direccion': 'Address',
    'dir': 'Address',
    
    'master_guide': 'Master Guide',
    'masterguide': 'Master Guide',
    'guia_master': 'Master Guide',
    
    'tariff_position': 'Tariff Position',
    'tariffposition': 'Tariff Position',
    'posicion_arancelaria': 'Tariff Position',
    
    'external_id': 'External Id',
    'externalid': 'External Id',
    'id_externo': 'External Id',
    
    # === COLUMNAS CXP ===
    'ot_number': 'OT Number',
    'otnumber': 'OT Number',
    'ot_num': 'OT Number',
    'numero_ot': 'OT Number',
    
    'date': 'Date',
    'fecha': 'Date',
    'datum': 'Date',
    'fecha_proceso': 'Date',
    
    'ref_': 'Ref #',
    'ref_number': 'Ref #',
    'ref_num': 'Ref #',
    'referencia': 'Ref #',
    'numero_referencia': 'Ref #',
    
    # Consignee ya definido arriba
    
    'co_aereo': 'CO Aereo',
    'coaereo': 'CO Aereo',
    'co_air': 'CO Aereo',
    'costo_aereo': 'CO Aereo',
    'aereo': 'CO Aereo',
    'cargo_aereo': 'CO Aereo',
    
    'arancel': 'Arancel',
    'tariff': 'Arancel',
    'aranceles': 'Arancel',
    'duty': 'Arancel',
    
    # IVA ya definido arriba
    
    'handling': 'Handling',
    'manejo': 'Handling',
    'manipulacion': 'Handling',
    'costo_manejo': 'Handling',
    
    'dest_delivery': 'Dest. Delivery',
    'destdelivery': 'Dest. Delivery',
    'entrega_destino': 'Dest. Delivery',
    'delivery': 'Dest. Delivery',
    'entrega': 'Dest. Delivery',
    
    'amt_due': 'Amt. Due',
    'amtdue': 'Amt. Due',
    'amount_due': 'Amt. Due',
    'monto_debido': 'Amt. Due',
    'due': 'Amt. Due',
    'adeudado': 'Amt. Due',
    
    'goods_value': 'Goods Value',
    'goodsvalue': 'Goods Value',
    'valor_mercancia': 'Goods Value',
    'value': 'Goods Value',
    'valor_bienes': 'Goods Value',
    
    # === COLUMNAS ADITIONALS ===
    'order_id': 'Order Id',  # Para Aditionals específicamente
    'orderid': 'Order Id',
    'numero_orden': 'Order Id',
    
    'item': 'Item',
    'articulo': 'Item',
    'producto': 'Item',
    'sku': 'Item',
    
    # Reference, Description, Quantity ya definidas arriba
    
    'unitprice': 'UnitPrice',
    'unit_price': 'UnitPrice',
    'precio_unitario': 'UnitPrice',
    'price_unit': 'UnitPrice',
    
    # Total ya definido arriba
}

def detect_file_type_intelligent(df):
    """
    Detecta inteligentemente el tipo de archivo basado en las columnas presentes
    """
    columns_lower = [normalize_column_name(col) for col in df.columns]
    
    # Patrones únicos para identificar cada tipo de archivo
    drapify_indicators = ['order_id', 'serial_', 'system_', 'account_name', 'client_first_name', 'client_last_name']
    logistics_indicators = ['reference', 'guide_number', 'fob', 'shipper', 'master_guide', 'tariff_position']
    cxp_indicators = ['ref_', 'ot_number', 'co_aereo', 'arancel', 'amt_due', 'goods_value']
    aditionals_indicators = ['order_id', 'item', 'unitprice', 'description'] # Order Id específico de Aditionals
    
    # Contar coincidencias exactas
    drapify_score = sum(1 for indicator in drapify_indicators if any(indicator in col for col in columns_lower))
    logistics_score = sum(1 for indicator in logistics_indicators if any(indicator in col for col in columns_lower))
    cxp_score = sum(1 for indicator in cxp_indicators if any(indicator in col for col in columns_lower))
    aditionals_score = sum(1 for indicator in aditionals_indicators if any(indicator in col for col in columns_lower))
    
    # Casos especiales de detección
    # Si tiene Order Id + Item + UnitPrice = Aditionals
    if any('order_id' in col for col in columns_lower) and any('item' in col for col in columns_lower) and any('unitprice' in col for col in columns_lower):
        aditionals_score += 3
    
    # Si tiene Ref # + CO Aereo = CXP
    if any('ref_' in col for col in columns_lower) and any('co_aereo' in col for col in columns_lower):
        cxp_score += 3
    
    # Si tiene Reference + Guide Number = Logistics
    if any('reference' in col for col in columns_lower) and any('guide' in col for col in columns_lower):
        logistics_score += 3
    
    # Si tiene account_name + Serial# = Drapify
    if any('account' in col for col in columns_lower) and any('serial' in col for col in columns_lower):
        drapify_score += 3
    
    # Determinar tipo basado en mayor puntuación
    scores = {
        'drapify': drapify_score,
        'logistics': logistics_score, 
        'cxp': cxp_score,
        'aditionals': aditionals_score
    }
    
    detected_type = max(scores.keys(), key=lambda k: scores[k])
    max_score = scores[detected_type]
    confidence = max_score / 6  # Normalizar a 0-1
    
    return detected_type, confidence, scores

def find_similar_column(target, available_columns, threshold=0.8):
    """
    Encuentra la columna más similar usando algoritmos de similitud
    Para casos donde no hay match exacto
    """
    target_norm = normalize_column_name(target)
    best_match = None
    best_score = 0
    
    for col in available_columns:
        col_norm = normalize_column_name(col)
        
        # Calcular similitud usando SequenceMatcher
        similarity = SequenceMatcher(None, target_norm, col_norm).ratio()
        
        if similarity > best_score and similarity >= threshold:
            best_score = similarity
            best_match = col
    
    return best_match, best_score

def map_columns_flexible(df, file_type="unknown"):
    """
    Mapea columnas de forma flexible manejando todas las variaciones
    
    Args:
        df: DataFrame a procesar
        file_type: Tipo de archivo detectado ("drapify", "logistics", "cxp", "aditionals")
    
    Returns:
        DataFrame con columnas renombradas
    """
    st.info(f"🔧 Aplicando mapeo flexible para archivo tipo: {file_type}")
    
    # Crear diccionario normalizado para búsqueda rápida
    normalized_mapping = {}
    for variation, standard in FLEXIBLE_COLUMN_MAPPING.items():
        normalized_key = normalize_column_name(variation)
        normalized_mapping[normalized_key] = standard
    
    # Mapear columnas del DataFrame
    new_columns = {}
    matched_columns = []
    unmatched_columns = []
    
    for original_col in df.columns:
        normalized_col = normalize_column_name(original_col)
        
        # Búsqueda exacta primero
        if normalized_col in normalized_mapping:
            standard_name = normalized_mapping[normalized_col]
            new_columns[original_col] = standard_name
            matched_columns.append(f"'{original_col}' → '{standard_name}'")
        else:
            # Intentar matching por similitud si no hay match exacto
            best_match, score = find_similar_column(normalized_col, normalized_mapping.keys())
            if best_match and score >= 0.8:
                standard_name = normalized_mapping[best_match]
                new_columns[original_col] = standard_name
                matched_columns.append(f"'{original_col}' → '{standard_name}' (similitud: {score:.2f})")
            else:
                # Si no hay match, mantener nombre original
                unmatched_columns.append(original_col)
    
    # Mostrar resultados del mapeo
    if matched_columns:
        st.success(f"✅ Columnas mapeadas exitosamente ({len(matched_columns)}):")
        # Mostrar máximo 10 para no saturar la interfaz
        display_matches = matched_columns[:10]
        for match in display_matches:
            st.write(f"   {match}")
        if len(matched_columns) > 10:
            st.write(f"   ... y {len(matched_columns) - 10} columnas más")
    
    if unmatched_columns:
        st.warning(f"⚠️ Columnas sin mapeo estándar ({len(unmatched_columns)}):")
        # Mostrar máximo 5 para no saturar
        display_unmatched = unmatched_columns[:5] 
        for col in display_unmatched:
            st.write(f"   '{col}' (se mantiene nombre original)")
        if len(unmatched_columns) > 5:
            st.write(f"   ... y {len(unmatched_columns) - 5} más")
    
    # Aplicar renombrado
    df_renamed = df.rename(columns=new_columns)
    
    st.success(f"🎯 Mapeo flexible completado: {len(new_columns)} columnas renombradas")
    
    return df_renamed

# REEMPLAZO PARA TU FUNCIÓN PRINCIPAL
def process_files_according_to_rules_FLEXIBLE(drapify_df, logistics_df=None, aditionals_df=None, cxp_df=None):
    """
    Versión mejorada de process_files_according_to_rules con mapeo flexible de columnas
    REEMPLAZA tu función original
    """
    
    st.info("🔄 Iniciando consolidación con MAPEO FLEXIBLE de columnas...")
    
    # PASO 0: Detectar tipos de archivo y aplicar mapeo flexible
    st.info("🔧 Detectando tipos de archivo y aplicando mapeo flexible...")
    
    # Detectar y mapear Drapify (archivo base)
    drapify_type, confidence, scores = detect_file_type_intelligent(drapify_df)
    st.info(f"📊 Archivo base detectado como: **{drapify_type}** (confianza: {confidence:.2f})")
    if confidence < 0.5:
        st.warning(f"⚠️ Baja confianza en detección. Puntuaciones: {scores}")
    
    consolidated_df = map_columns_flexible(drapify_df, drapify_type)
    st.success(f"✅ Archivo base procesado: {len(consolidated_df)} registros")
    
    # Mapear archivos opcionales con detección automática
    if logistics_df is not None and not logistics_df.empty:
        st.info("🚚 Procesando archivo Logistics...")
        logistics_type, confidence, scores = detect_file_type_intelligent(logistics_df)
        st.info(f"🚚 Detectado como: **{logistics_type}** (confianza: {confidence:.2f})")
        logistics_df = map_columns_flexible(logistics_df, logistics_type)
    
    if aditionals_df is not None and not aditionals_df.empty:
        st.info("➕ Procesando archivo Aditionals...")
        aditionals_type, confidence, scores = detect_file_type_intelligent(aditionals_df)
        st.info(f"➕ Detectado como: **{aditionals_type}** (confianza: {confidence:.2f})")
        aditionals_df = map_columns_flexible(aditionals_df, aditionals_type)
    
    if cxp_df is not None and not cxp_df.empty:
        st.info("💰 Procesando archivo CXP...")
        cxp_type, confidence, scores = detect_file_type_intelligent(cxp_df)
        st.info(f"💰 Detectado como: **{cxp_type}** (confianza: {confidence:.2f})")
        cxp_df = map_columns_flexible(cxp_df, cxp_type)
    
    st.success("🎉 Mapeo flexible completado para todos los archivos!")
    
    # RESTO DEL PROCESAMIENTO (tu lógica original continúa aquí...)
    st.info("🔄 Continuando con lógica de consolidación original...")
    
    # PASO 2: Procesar archivo Logistics si está disponible (tu código original)
    if logistics_df is not None and not logistics_df.empty:
        st.info("🚚 Consolidando datos de Logistics...")
        
        # Crear diccionario para mapeo rápido de Logistics
        logistics_dict_by_reference = {}
        logistics_dict_by_order_number = {}
        
        for idx, row in logistics_df.iterrows():
            # Limpiar los IDs para mejor matching
            reference = clean_id(row.get('Reference', ''))
            order_number = clean_id(row.get('Order number', ''))
            
            if reference:
                logistics_dict_by_reference[reference] = row
            if order_number:
                logistics_dict_by_order_number[order_number] = row
        
        st.info(f"📋 Logistics indexado: {len(logistics_dict_by_reference)} por Reference, {len(logistics_dict_by_order_number)} por Order number")
        
        # Agregar columnas de Logistics al DataFrame consolidado
        logistics_columns = [
            'Guide Number', 'Order number', 'Reference', 'SAP Code', 'Invoice', 
            'Status', 'FOB', 'Unit', 'Weight', 'Length', 'Width', 'Height',
            'Insurance', 'Logistics', 'Duties Prealert', 'Duties Pay', 
            'Duty Fee', 'Saving', 'Total', 'Description', 'Shipper', 'Phone',
            'Consignee', 'Identification', 'Country', 'State', 'City', 
            'Address', 'Master Guide', 'Tariff Position', 'External Id', 'Invoice'
        ]
        
        # Inicializar columnas de Logistics con NaN
        for col in logistics_columns:
            if col in logistics_df.columns:
                consolidated_df[f'logistics_{col.lower().replace(" ", "_")}'] = np.nan
        
        matched_by_order_id = 0
        matched_by_prealert_id = 0
        
        # Hacer el matching según las reglas
        for idx, row in consolidated_df.iterrows():
            # Limpiar los IDs para mejor matching
            order_id = clean_id(row.get('order_id', ''))
            prealert_id = clean_id(row.get('prealert_id', ''))
            
            logistics_row = None
            match_type = None
            
            # Regla 1: Buscar order_id en Reference
            if order_id and order_id in logistics_dict_by_reference:
                logistics_row = logistics_dict_by_reference[order_id]
                matched_by_order_id += 1
                match_type = "order_id->Reference"
            
            # Regla 2: Si no encuentra, buscar prealert_id en Order number
            elif prealert_id and prealert_id in logistics_dict_by_order_number:
                logistics_row = logistics_dict_by_order_number[prealert_id]
                matched_by_prealert_id += 1
                match_type = "prealert_id->Order number"
            
            # Si encontró match, copiar los datos
            if logistics_row is not None:
                for col in logistics_columns:
                    if col in logistics_df.columns:
                        consolidated_df.loc[idx, f'logistics_{col.lower().replace(" ", "_")}'] = logistics_row.get(col)
                
                # Debug: mostrar algunos matches
                if (matched_by_order_id + matched_by_prealert_id) <= 5:
                    st.write(f"✅ Match {matched_by_order_id + matched_by_prealert_id}: {match_type} - order_id: {order_id}, prealert_id: {prealert_id}")
        
        st.success(f"✅ Logistics procesado: {matched_by_order_id} matches por order_id, {matched_by_prealert_id} matches por prealert_id")
    
    # PASO 3: Procesar archivo Aditionals si está disponible (tu código original)
    if aditionals_df is not None and not aditionals_df.empty:
        st.info("➕ Consolidando datos de Aditionals...")
        
        # Crear diccionario para mapeo rápido de Aditionals
        aditionals_dict = {}
        for idx, row in aditionals_df.iterrows():
            order_id = clean_id(row.get('Order Id', ''))
            if order_id:
                aditionals_dict[order_id] = row
        
        st.info(f"📋 Aditionals indexado: {len(aditionals_dict)} registros")
        
        # Agregar columnas de Aditionals
        aditionals_columns = ['Order Id', 'Item', 'Reference', 'Description', 'Quantity', 'UnitPrice', 'Total']
        
        for col in aditionals_columns:
            if col in aditionals_df.columns:
                consolidated_df[f'aditionals_{col.lower().replace(" ", "_")}'] = np.nan
        
        matched_aditionals = 0
        
        # Hacer matching por prealert_id -> Order Id
        for idx, row in consolidated_df.iterrows():
            prealert_id = clean_id(row.get('prealert_id', ''))
            
            if prealert_id and prealert_id in aditionals_dict:
                aditionals_row = aditionals_dict[prealert_id]
                matched_aditionals += 1
                
                for col in aditionals_columns:
                    if col in aditionals_df.columns:
                        consolidated_df.loc[idx, f'aditionals_{col.lower().replace(" ", "_")}'] = aditionals_row.get(col)
                
                # Debug: mostrar algunos matches
                if matched_aditionals <= 5:
                    st.write(f"✅ Aditional Match {matched_aditionals}: prealert_id {prealert_id} encontrado")
        
        st.success(f"✅ Aditionals procesado: {matched_aditionals} matches por prealert_id")
    
    # PASO 4: Calcular columna Asignacion (tu código original)
    st.info("🏷️ Calculando columna Asignacion...")
    
    if 'account_name' in consolidated_df.columns and 'Serial#' in consolidated_df.columns:
        consolidated_df['Asignacion'] = consolidated_df.apply(
            lambda row: calculate_asignacion(row['account_name'], row['Serial#']), 
            axis=1
        )
        asignaciones_calculadas = consolidated_df['Asignacion'].notna().sum()
        st.success(f"✅ Asignaciones calculadas: {asignaciones_calculadas}")
    else:
        st.warning("⚠️ No se pudo calcular Asignacion: faltan columnas account_name o Serial#")
    
    # PASO 5: Procesar archivo CXP si está disponible (tu código original)
    if cxp_df is not None and not cxp_df.empty:
        st.info("💰 Consolidando datos de CXP...")
        
        # Mostrar las columnas del archivo CXP para debugging
        st.write(f"🔍 Columnas encontradas en CXP: {list(cxp_df.columns)}")
        
        # Crear diccionario para mapeo rápido de CXP
        cxp_dict = {}
        for idx, row in cxp_df.iterrows():
            ref_number = clean_id(row.get('Ref #', ''))
            if ref_number:
                cxp_dict[ref_number] = row
        
        st.info(f"📋 CXP indexado: {len(cxp_dict)} registros")
        
        # Mostrar algunos ejemplos de Ref # para debug
        cxp_refs = list(cxp_dict.keys())[:5]
        st.write(f"🔍 Ejemplos de Ref # en CXP: {cxp_refs}")
        
        # Agregar columnas de CXP (usar todas las columnas disponibles)
        available_cxp_columns = []
        standard_cxp_columns = ['OT Number', 'Date', 'Ref #', 'Consignee', 'CO Aereo', 
                               'Arancel', 'IVA', 'Handling', 'Dest. Delivery', 'Amt. Due', 'Goods Value']
        
        for col in standard_cxp_columns:
            if col in cxp_df.columns:
                available_cxp_columns.append(col)
                consolidated_df[f'cxp_{col.lower().replace(" ", "_").replace(".", "").replace("#", "number")}'] = np.nan
        
        st.write(f"📊 Columnas CXP que se procesarán: {available_cxp_columns}")
        
        matched_cxp = 0
        
        # Hacer matching por Asignacion -> Ref #
        if 'Asignacion' in consolidated_df.columns:
            # Mostrar algunos ejemplos de Asignacion para debug
            asignaciones = consolidated_df['Asignacion'].dropna().head(5).tolist()
            st.write(f"🔍 Ejemplos de Asignacion calculadas: {asignaciones}")
            
            for idx, row in consolidated_df.iterrows():
                asignacion = clean_id(row.get('Asignacion', ''))
                
                if asignacion and asignacion in cxp_dict:
                    cxp_row = cxp_dict[asignacion]
                    matched_cxp += 1
                    
                    for col in available_cxp_columns:
                        col_name = f'cxp_{col.lower().replace(" ", "_").replace(".", "").replace("#", "number")}'
                        consolidated_df.loc[idx, col_name] = cxp_row.get(col)
                    
                    # Debug: mostrar algunos matches
                    if matched_cxp <= 5:
                        st.write(f"✅ CXP Match {matched_cxp}: Asignacion '{asignacion}' encontrada")
        
        st.success(f"✅ CXP procesado: {matched_cxp} matches por Asignacion")
    
    # PASO 6: Aplicar solo formatos básicos (tu función original)
    consolidated_df = apply_basic_formatting(consolidated_df)
    
    # PASO 7: Validación de duplicados por order_id (tu código original)
    st.info("🔍 Validando duplicados por order_id...")
    
    if 'order_id' in consolidated_df.columns:
        initial_count = len(consolidated_df)
        # Eliminar duplicados manteniendo el primer registro
        consolidated_df = consolidated_df.drop_duplicates(subset=['order_id'], keep='first')
        final_count = len(consolidated_df)
        
        if initial_count != final_count:
            removed_count = initial_count - final_count
            st.warning(f"⚠️ Se removieron {removed_count} registros duplicados por order_id")
        else:
            st.success("✅ No se encontraron duplicados por order_id")
    
    st.success(f"🎉 Consolidación flexible completada: {len(consolidated_df)} registros finales")
    return consolidated_df

# FUNCIÓN HELPER QUE YA TIENES (asegúrate de que esté disponible)
def clean_id(value):
    """Limpia y normaliza IDs removiendo comillas y espacios"""
    if pd.isna(value):
        return None
    str_value = str(value).strip()
    # Remover comilla simple al inicio si existe
    if str_value.startswith("'"):
        str_value = str_value[1:]
    # Remover .0 al final si es un número entero
    if str_value.endswith('.0'):
        str_value = str_value[:-2]
    return str_value if str_value and str_value != 'nan' else None

def calculate_asignacion(account_name, serial_number):
    """Calcula la asignación basada en el account_name y serial_number"""
    if pd.isna(account_name) or pd.isna(serial_number):
        return None
    
    # Limpiar serial_number para evitar decimales
    clean_serial = clean_id(serial_number)
    if not clean_serial:
        return None
    
    # Mapeo exacto según las especificaciones
    account_mapping = {
        '1-TODOENCARGO-CO': 'TDC',
        '2-MEGATIENDA SPA': 'MEGA',
        '4-MEGA TIENDAS PERUANAS': 'MGA-PE',
        '5-DETODOPARATODOS': 'DTPT',
        '6-COMPRAFACIL': 'CFA',
        '7-COMPRA-YA': 'CPYA',
        '8-FABORCARGO': 'FBC',
        '3-VEENDELO': 'VEEN'
    }
    
    prefix = account_mapping.get(account_name, '')
    return f"{prefix}{clean_serial}" if prefix else clean_serial

def apply_basic_formatting(df):
    """Aplica formatos básicos sin afectar campos numéricos para BD"""
    
    st.info("🔧 Aplicando formatos básicos para base de datos...")
    
    # C) Corregir encoding en columnas de texto
    text_columns = [
        'client_first_name', 'client_last_name', 'title', 'address_line', 
        'street_name', 'city', 'state', 'country', 'nombre_del_tercero',
        'direccion', 'apelido_del_tercero', 'estado', 'razon_social', 'ciudad',
        'logistics_description', 'logistics_shipper', 'logistics_consignee',
        'logistics_country', 'logistics_state', 'logistics_city', 'logistics_address'
    ]
    
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].apply(fix_encoding)
    
    # D) Formatear fechas
    date_columns = {
        'date_created': 'datetime',  # YYYY-MM-DD HH:MM
        'cxp_date': 'cxp_format'     # MM/DD/YYYY del archivo CXP
    }
    
    for col, format_type in date_columns.items():
        if col in df.columns:
            df[col] = df[col].apply(format_date_standard)
    
    st.success("✅ Formatos básicos aplicados")
    return df

def fix_encoding(text):
    """Corrige caracteres mal codificados automáticamente"""
    if pd.isna(text) or not isinstance(text, str):
        return text
    
    try:
        # Intentar corregir encoding automáticamente si contiene caracteres problemáticos
        if 'Ã' in text:
            # Codificar como latin-1 y decodificar como utf-8
            fixed = text.encode('latin-1').decode('utf-8')
            return fixed
    except:
        pass
    
    return text

def format_date_standard(date_value, input_format="auto"):
    """Convierte fechas a formato YYYY-MM-DD usando manipulación de strings"""
    if pd.isna(date_value) or date_value == "":
        return None
    
    date_str = str(date_value).strip()
    
    try:
        # Formato: YYYY-MM-DD HH:MM (2025-07-21 21:49) -> YYYY-MM-DD
        if re.match(r'\d{4}-\d{2}-\d{2}\s', date_str):
            return date_str.split(' ')[0]
        
        # Formato: MM/DD/YYYY (07/23/2025) -> YYYY-MM-DD
        if re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_str):
            parts = date_str.split('/')
            if len(parts) == 3:
                month = parts[0].zfill(2)
                day = parts[1].zfill(2)
                year = parts[2]
                return f"{year}-{month}-{day}"
        
        # Si ya está en formato YYYY-MM-DD, devolverlo como está
        if re.match(r'\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
            
    except:
        pass
    
    return date_str  # Si no se puede convertir, devolver original
