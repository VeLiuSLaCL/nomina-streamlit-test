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


def construir_orden_columnas(columnas_base, columnas_dinamicas):
    # Bloques prioritarios pedidos
    bloque_sueldo = [
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

    bloque_festivo = [
        "FESTIVO LABORADO GRAVADO",
        "FESTIVO LABORADO EXENTO",
        "DESCANSO LABORADO GRAVADO",
        "DESCANSO LABORADO EXENTO",
    ]

    columnas_usadas = set()
    orden_final = []

    # 1) Bloque sueldo en orden exacto
    for col in bloque_sueldo:
        if col in columnas_dinamicas:
            orden_final.append(col)
            columnas_usadas.add(col)

    # 2) Totales sueldo
    orden_final.extend(["TOTAL_SUELDO_GRAVADO", "TOTAL_SUELDO_EXENTO"])
    columnas_usadas.update(["TOTAL_SUELDO_GRAVADO", "TOTAL_SUELDO_EXENTO"])

    # 3) Bloque festivo en orden exacto
    for col in bloque_festivo:
        if col in columnas_dinamicas:
            orden_final.append(col)
            columnas_usadas.add(col)

    # 4) Totales festivo
    orden_final.extend(["TOTAL_FESTIVO_GRAVADO", "TOTAL_FESTIVO_EXENTO"])
    columnas_usadas.update(["TOTAL_FESTIVO_GRAVADO", "TOTAL_FESTIVO_EXENTO"])

    # 5) Resto: primero GRAVADO, luego EXENTO
    restantes = [c for c in columnas_dinamicas if c not in columnas_usadas]

    restantes_gravado = sorted(
        [c for c in restantes if str(c).endswith(" GRAVADO")],
        key=lambda x: x.upper()
    )
    restantes_exento = sorted(
        [c for c in restantes if str(c).endswith(" EXENTO")],
        key=lambda x: x.upper()
    )

    # Columnas no dinámicas extra por si existieran
    restantes_otras = sorted(
        [c for c in restantes if not str(c).endswith(" GRAVADO") and not str(c).endswith(" EXENTO")],
        key=lambda x: x.upper()
    )

    orden_final.extend(restantes_gravado)
    orden_final.extend(restantes_exento)
    orden_final.extend(restantes_otras)

    # 6) Totales generales siempre al final
    orden_final.extend(["TOTAL_EXENTO", "TOTAL_GRAVADO"])

    return columnas_base + orden_final


def transformar_bloque(df_bloque, columnas_base, col_concepto_detalle, col_exento, col_gravado):
    if df_bloque.empty:
        columnas_finales = columnas_base + [
            "TOTAL_SUELDO_GRAVADO",
            "TOTAL_SUELDO_EXENTO",
            "TOTAL_FESTIVO_GRAVADO",
            "TOTAL_FESTIVO_EXENTO",
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

    # Asegurar columnas clave para que siempre existan
    columnas_clave = [
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
        "FESTIVO LABORADO GRAVADO",
        "FESTIVO LABORADO EXENTO",
        "DESCANSO LABORADO GRAVADO",
        "DESCANSO LABORADO EXENTO",
    ]

    for col in columnas_clave:
        asegurar_columna(resultado, col)

    # Totales parciales solicitados
    resultado["TOTAL_SUELDO_GRAVADO"] = sumar_columnas(resultado, [
        "SUELDO GRAVADO",
        "Cantidad pendiente GRAVADO",
        "AUSENCIA INJUSTIFICADA GRAVADO",
        "PERMISO SIN GOCE GRAVADO",
        "INCAPACIDAD E GRAL GRAVADO",
        "Ctdad pendiente mes ant GRAVADO",
    ])

    resultado["TOTAL_SUELDO_EXENTO"] = sumar_columnas(resultado, [
        "SUELDO EXENTO",
        "Cantidad pendiente EXENTO",
        "AUSENCIA INJUSTIFICADA EXENTO",
        "PERMISO SIN GOCE EXENTO",
        "INCAPACIDAD E GRAL EXENTO",
        "Ctdad pendiente mes ant EXENTO",
    ])

    resultado["TOTAL_FESTIVO_GRAVADO"] = sumar_columnas(resultado, [
        "FESTIVO LABORADO GRAVADO",
        "DESCANSO LABORADO GRAVADO",
    ])

    resultado["TOTAL_FESTIVO_EXENTO"] = sumar_columnas(resultado, [
        "FESTIVO LABORADO EXENTO",
        "DESCANSO LABORADO EXENTO",
    ])

    # Totales generales
    cols_exento = [c for c in resultado.columns if str(c).endswith(" EXENTO")]
    cols_gravado = [c for c in resultado.columns if str(c).endswith(" GRAVADO")]

    resultado["TOTAL_EXENTO"] = resultado[cols_exento].sum(axis=1) if cols_exento else 0.0
    resultado["TOTAL_GRAVADO"] = resultado[cols_gravado].sum(axis=1) if cols_gravado else 0.0

    columnas_dinamicas = [c for c in resultado.columns if c not in columnas_base]
    orden_final = construir_orden_columnas(columnas_base, columnas_dinamicas)

    # Asegurar que todas existan antes de reordenar
    for col in orden_final:
        asegurar_columna(resultado, col)

    resultado = resultado[orden_final]

    return resultado


def transformar_hoja_nomina(archivo, nombre_hoja):
    archivo.seek(0)
    hoja_original_df = pd.read_excel(archivo, sheet_name=nombre_hoja)
    df = hoja_original_df.copy()

    df.columns = [str(c).strip() for c in df.columns]

    # División exclusivamente desde columna N = CONCEPTO
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
