import re
import pandas as pd


def normalizar_texto(valor):
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def leer_hojas_excel(archivo):
    archivo.seek(0)
    xls = pd.ExcelFile(archivo)
    return xls.sheet_names


def buscar_columna(df, candidatos_exactos=None):
    candidatos_exactos = candidatos_exactos or []
    columnas = list(df.columns)
    columnas_norm = {str(c).strip().lower(): c for c in columnas}

    for nombre in candidatos_exactos:
        key = str(nombre).strip().lower()
        if key in columnas_norm:
            return columnas_norm[key]

    return None


def clasificar_desde_columna_concepto(valor):
    texto = normalizar_texto(valor).upper()

    if "DEDUC" in texto:
        return "DEDUCCIONES"

    if "PERCEP" in texto:
        return "PERCEPCIONES"

    return None


def obtener_columnas_base(df):
    preferidas = [
        "Período cál.nómina",
        "Año de nómina",
        "Mes",
        "Nº de secuencia",
        "Número de personal",
        "Sociedad",
        "Área de nómina",
        "Tipo de nómina",
        "Identificador de nómina",
        "Motivo nóm.especial",
        "Nº ejecución contabil.",
        "Estado Impuesto Estatal",
        "Folio CFDi",
        "Nombre de Serie",
        "Fecha Timbrado",
        "Fecha de Pago",
        "UUID",
    ]

    existentes = [c for c in preferidas if c in df.columns]

    if not existentes:
        raise ValueError(
            "No se encontraron columnas base suficientes para consolidar por empleado y período."
        )

    return existentes


def asegurar_columna(df, nombre_columna):
    if nombre_columna not in df.columns:
        df[nombre_columna] = 0.0


def sumar_columnas(df, columnas):
    existentes = [c for c in columnas if c in df.columns]
    if not existentes:
        return pd.Series([0.0] * len(df), index=df.index)
    return df[existentes].sum(axis=1)


def normalizar_clave(texto):
    texto = normalizar_texto(texto).upper()
    texto = re.sub(r"\s+", " ", texto)
    return texto


def separar_base_y_tipo(columna):
    col = normalizar_texto(columna)
    if col.upper().endswith(" GRAVADO"):
        base = col[:-9]
        return normalizar_clave(base), "GRAVADO"
    if col.upper().endswith(" EXENTO"):
        base = col[:-8]
        return normalizar_clave(base), "EXENTO"
    return normalizar_clave(col), None


def ordenar_columnas_por_concepto(columnas_dinamicas):
    pares = {}
    otras = []

    for col in columnas_dinamicas:
        base_norm, tipo = separar_base_y_tipo(col)

        if tipo == "GRAVADO":
            pares.setdefault(base_norm, {})["GRAVADO"] = col
        elif tipo == "EXENTO":
            pares.setdefault(base_norm, {})["EXENTO"] = col
        else:
            otras.append(col)

    ordenadas = []
    for base in sorted(pares.keys()):
        if "GRAVADO" in pares[base]:
            ordenadas.append(pares[base]["GRAVADO"])
        if "EXENTO" in pares[base]:
            ordenadas.append(pares[base]["EXENTO"])

    otras = sorted(otras, key=lambda x: normalizar_clave(x))

    return ordenadas + otras


def seleccionar_columnas_existentes(columnas_dinamicas, lista_objetivo):
    """
    Busca columnas aunque cambien espacios/mayúsculas.
    """
    mapa = {}
    for col in columnas_dinamicas:
        base_norm, tipo = separar_base_y_tipo(col)
        clave = f"{base_norm}|{tipo}" if tipo else base_norm
        mapa[clave] = col

    resultado = []
    for objetivo in lista_objetivo:
        base_norm, tipo = separar_base_y_tipo(objetivo)
        clave = f"{base_norm}|{tipo}" if tipo else base_norm
        if clave in mapa:
            resultado.append(mapa[clave])

    return resultado


def construir_orden_final(columnas_base, columnas_dinamicas):
    bloque_inicial = [
        "SUELDO GRAVADO",
        "SUELDO EXENTO",
        "Cantidad pendiente GRAVADO",
        "Cantidad pendiente EXENTO",
        "AUSENCIA INJUSTIFICADA GRAVADO",
        "AUSENCIA INJUSTIFICADA EXENTO",
        "PERMISO SIN GOCE GRAVADO",
        "PERMISO SIN GOCE EXENTO",
        "INCAPACIDAD E GRAL GRAVADO",
        "INCAPACIDAD E GRAL EXENTO",
        "Ctdad pendiente mes ant GRAVADO",
        "Ctdad pendiente mes ant EXENTO",
    ]

    totales_especiales = [
        "TOTAL SUELDOS GRAVADO",
        "TOTAL SUELDOS EXENTO",
    ]

    bloque_festivo = [
        "FESTIVO LABORADO GRAVADO",
        "FESTIVO LABORADO EXENTO",
        "DESCANSO LABORADO GRAVADO",
        "DESCANSO LABORADO EXENTO",
    ]

    totales_festivo = [
        "TOTAL FESTIVO GRAVADO",
        "TOTAL FESTIVO EXENTO",
    ]

    bloque_vacaciones = [
        "HORAS EXTRAS DOBLES GRAVADO",
        "HORAS EXTRAS DOBLES EXENTO",
        "HORAS EXTRAS TRIPLES GRAVADO",
        "HORAS EXTRAS TRIPLES EXENTO",
        "PREMIO DE ASISTENCIA GRAVADO",
        "PREMIO DE ASISTENCIA EXENTO",
        "PREMIO DE PUNTUALIDAD GRAVADO",
        "PREMIO DE PUNTUALIDAD EXENTO",
        "LIQ VACACIONES GRAVADO",
        "LIQ VACACIONES EXENTO",
        "VACACIONES GRAVADO",
        "VACACIONES EXENTO",
    ]

    totales_vacaciones = [
        "TOTAL VACACIONES GRAVADO",
        "TOTAL VACACIONES EXENTO",
    ]

    bloque_prima_vacacional = [
        "PRIMA VACACIONAL GRAVADO",
        "PRIMA VACACIONAL EXENTO",
        "ExImp prima vacacional GRAVADO",
        "ExImp prima vacacional EXENTO",
        "LIQ PRIMA VACACIONAL M GRAVADO",
        "LIQ PRIMA VACACIONAL M EXENTO",
    ]

    totales_prima_vacacional = [
        "TOTAL PRIMA VACACIONAL GRAVADO",
        "TOTAL PRIMA VACACIONAL EXENTO",
    ]

    usadas = set()
    orden = []

    for grupo in [
        bloque_inicial,
        totales_especiales,
        bloque_festivo,
        totales_festivo,
        bloque_vacaciones,
        totales_vacaciones,
        bloque_prima_vacacional,
        totales_prima_vacacional,
    ]:
        cols = seleccionar_columnas_existentes(columnas_dinamicas, grupo)
        for col in cols:
            if col not in usadas:
                orden.append(col)
                usadas.add(col)

    restantes = [c for c in columnas_dinamicas if c not in usadas]
    orden.extend(ordenar_columnas_por_concepto(restantes))

    for total_general in ["TOTAL_EXENTO", "TOTAL_GRAVADO"]:
        if total_general in columnas_dinamicas and total_general not in orden:
            orden.append(total_general)

    return columnas_base + orden


def transformar_bloque(df_bloque, columnas_base, col_concepto_detalle, col_exento, col_gravado):
    if df_bloque.empty:
        columnas_finales = columnas_base + [
            "TOTAL SUELDOS GRAVADO",
            "TOTAL SUELDOS EXENTO",
            "TOTAL FESTIVO GRAVADO",
            "TOTAL FESTIVO EXENTO",
            "TOTAL VACACIONES GRAVADO",
            "TOTAL VACACIONES EXENTO",
            "TOTAL PRIMA VACACIONAL GRAVADO",
            "TOTAL PRIMA VACACIONAL EXENTO",
            "TOTAL_EXENTO",
            "TOTAL_GRAVADO",
        ]
        return pd.DataFrame(columns=columnas_finales)

    agrupado = (
        df_bloque.groupby(
            columnas_base + [col_concepto_detalle],
            dropna=False,
            as_index=False
        )[[col_exento, col_gravado]]
        .sum()
    )

    exento_pivot = agrupado.pivot(
        index=columnas_base,
        columns=col_concepto_detalle,
        values=col_exento
    ).fillna(0)

    gravado_pivot = agrupado.pivot(
        index=columnas_base,
        columns=col_concepto_detalle,
        values=col_gravado
    ).fillna(0)

    exento_pivot.columns = [f"{str(c).strip()} EXENTO" for c in exento_pivot.columns]
    gravado_pivot.columns = [f"{str(c).strip()} GRAVADO" for c in gravado_pivot.columns]

    resultado = pd.concat([gravado_pivot, exento_pivot], axis=1).reset_index()

    columnas_sueldos = [
        "SUELDO GRAVADO",
        "SUELDO EXENTO",
        "Cantidad pendiente GRAVADO",
        "Cantidad pendiente EXENTO",
        "AUSENCIA INJUSTIFICADA GRAVADO",
        "AUSENCIA INJUSTIFICADA EXENTO",
        "PERMISO SIN GOCE GRAVADO",
        "PERMISO SIN GOCE EXENTO",
        "INCAPACIDAD E GRAL GRAVADO",
        "INCAPACIDAD E GRAL EXENTO",
        "Ctdad pendiente mes ant GRAVADO",
        "Ctdad pendiente mes ant EXENTO",
    ]

    columnas_festivo = [
        "FESTIVO LABORADO GRAVADO",
        "DESCANSO LABORADO GRAVADO",
        "FESTIVO LABORADO EXENTO",
        "DESCANSO LABORADO EXENTO",
    ]

    columnas_vacaciones = [
        "LIQ VACACIONES GRAVADO",
        "VACACIONES GRAVADO",
        "LIQ VACACIONES EXENTO",
        "VACACIONES EXENTO",
    ]

    columnas_prima_vacacional = [
        "PRIMA VACACIONAL GRAVADO",
        "ExImp prima vacacional GRAVADO",
        "LIQ PRIMA VACACIONAL M GRAVADO",
        "PRIMA VACACIONAL EXENTO",
        "ExImp prima vacacional EXENTO",
        "LIQ PRIMA VACACIONAL M EXENTO",
    ]

    for col in columnas_sueldos + columnas_festivo + columnas_vacaciones + columnas_prima_vacacional:
        asegurar_columna(resultado, col)

    columnas_reales = list(resultado.columns)

    resultado["TOTAL SUELDOS GRAVADO"] = sumar_columnas(resultado, seleccionar_columnas_existentes(columnas_reales, [
        "SUELDO GRAVADO",
        "Cantidad pendiente GRAVADO",
        "AUSENCIA INJUSTIFICADA GRAVADO",
        "PERMISO SIN GOCE GRAVADO",
        "INCAPACIDAD E GRAL GRAVADO",
        "Ctdad pendiente mes ant GRAVADO",
    ]))

    resultado["TOTAL SUELDOS EXENTO"] = sumar_columnas(resultado, seleccionar_columnas_existentes(columnas_reales, [
        "SUELDO EXENTO",
        "Cantidad pendiente EXENTO",
        "AUSENCIA INJUSTIFICADA EXENTO",
        "PERMISO SIN GOCE EXENTO",
        "INCAPACIDAD E GRAL EXENTO",
        "Ctdad pendiente mes ant EXENTO",
    ]))

    resultado["TOTAL FESTIVO GRAVADO"] = sumar_columnas(resultado, seleccionar_columnas_existentes(columnas_reales, [
        "FESTIVO LABORADO GRAVADO",
        "DESCANSO LABORADO GRAVADO",
    ]))

    resultado["TOTAL FESTIVO EXENTO"] = sumar_columnas(resultado, seleccionar_columnas_existentes(columnas_reales, [
        "FESTIVO LABORADO EXENTO",
        "DESCANSO LABORADO EXENTO",
    ]))

    resultado["TOTAL VACACIONES GRAVADO"] = sumar_columnas(resultado, seleccionar_columnas_existentes(columnas_reales, [
        "LIQ VACACIONES GRAVADO",
        "VACACIONES GRAVADO",
    ]))

    resultado["TOTAL VACACIONES EXENTO"] = sumar_columnas(resultado, seleccionar_columnas_existentes(columnas_reales, [
        "LIQ VACACIONES EXENTO",
        "VACACIONES EXENTO",
    ]))

    resultado["TOTAL PRIMA VACACIONAL GRAVADO"] = sumar_columnas(resultado, seleccionar_columnas_existentes(columnas_reales, [
        "PRIMA VACACIONAL GRAVADO",
        "ExImp prima vacacional GRAVADO",
        "LIQ PRIMA VACACIONAL M GRAVADO",
    ]))

    resultado["TOTAL PRIMA VACACIONAL EXENTO"] = sumar_columnas(resultado, seleccionar_columnas_existentes(columnas_reales, [
        "PRIMA VACACIONAL EXENTO",
        "ExImp prima vacacional EXENTO",
        "LIQ PRIMA VACACIONAL M EXENTO",
    ]))

    cols_exento = [c for c in resultado.columns if str(c).endswith(" EXENTO")]
    cols_gravado = [c for c in resultado.columns if str(c).endswith(" GRAVADO")]

    resultado["TOTAL_EXENTO"] = resultado[cols_exento].sum(axis=1) if cols_exento else 0.0
    resultado["TOTAL_GRAVADO"] = resultado[cols_gravado].sum(axis=1) if cols_gravado else 0.0

    columnas_dinamicas = [c for c in resultado.columns if c not in columnas_base]

    orden_final = construir_orden_final(columnas_base, columnas_dinamicas)

    for col in orden_final:
        asegurar_columna(resultado, col)

    resultado = resultado[orden_final]

    return resultado


def transformar_hoja_nomina(archivo, nombre_hoja):
    archivo.seek(0)
    hoja_original_df = pd.read_excel(archivo, sheet_name=nombre_hoja)
    df = hoja_original_df.copy()

    df.columns = [str(c).strip() for c in df.columns]

    col_concepto = buscar_columna(df, candidatos_exactos=["CONCEPTO"])
    if not col_concepto:
        raise ValueError("No encontré la columna 'CONCEPTO' (columna N).")

    col_concepto_detalle = buscar_columna(
        df,
        candidatos_exactos=["Texto expl.CC-nómina", "Texto expl.CC-nomina"]
    )
    col_exento = buscar_columna(df, candidatos_exactos=["Exento"])
    col_gravado = buscar_columna(df, candidatos_exactos=["Gravado"])

    if not col_concepto_detalle:
        raise ValueError("No encontré la columna 'Texto expl.CC-nómina'.")
    if not col_exento:
        raise ValueError("No encontré la columna 'Exento'.")
    if not col_gravado:
        raise ValueError("No encontré la columna 'Gravado'.")

    columnas_base = obtener_columnas_base(df)

    df[col_concepto] = df[col_concepto].apply(normalizar_texto)
    df[col_concepto_detalle] = df[col_concepto_detalle].apply(normalizar_texto)
    df[col_exento] = pd.to_numeric(df[col_exento], errors="coerce").fillna(0.0)
    df[col_gravado] = pd.to_numeric(df[col_gravado], errors="coerce").fillna(0.0)

    df["_TIPO_SALIDA_"] = df[col_concepto].apply(clasificar_desde_columna_concepto)

    df = df[df["_TIPO_SALIDA_"].notna()].copy()
    df = df[df[col_concepto_detalle].str.strip() != ""].copy()

    percepciones_df = df[df["_TIPO_SALIDA_"] == "PERCEPCIONES"].copy()
    deducciones_df = df[df["_TIPO_SALIDA_"] == "DEDUCCIONES"].copy()

    percepciones_final = transformar_bloque(
        percepciones_df,
        columnas_base,
        col_concepto_detalle,
        col_exento,
        col_gravado
    )

    deducciones_final = transformar_bloque(
        deducciones_df,
        columnas_base,
        col_concepto_detalle,
        col_exento,
        col_gravado
    )

    nombre_salida = "nomina_transformada.xlsx"
    return hoja_original_df, percepciones_final, deducciones_final, nombre_salida
