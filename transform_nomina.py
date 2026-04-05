import pandas as pd


def normalizar_texto(valor):
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def leer_hojas_excel(archivo):
    archivo.seek(0)
    xls = pd.ExcelFile(archivo)
    return xls.sheet_names


def buscar_columna(df, candidatos_exactos=None, contiene=None, preferir_primera=True):
    candidatos_exactos = candidatos_exactos or []
    contiene = contiene or []

    columnas = list(df.columns)
    columnas_norm = {str(c).strip().lower(): c for c in columnas}

    for nombre in candidatos_exactos:
        key = str(nombre).strip().lower()
        if key in columnas_norm:
            return columnas_norm[key]

    coincidencias = []
    for c in columnas:
        c_norm = str(c).strip().lower()
        for fragmento in contiene:
            if fragmento.lower() in c_norm:
                coincidencias.append(c)
                break

    if coincidencias:
        return coincidencias[0] if preferir_primera else coincidencias[-1]

    return None


def clasificar_registro(valor_concepto, valor_tipo_concepto):
    texto1 = normalizar_texto(valor_concepto).upper()
    texto2 = normalizar_texto(valor_tipo_concepto).upper()

    if "DEDUC" in texto1 or "DEDUC" in texto2:
        return "DEDUCCIONES"

    if "PERCEP" in texto1 or "PERCEP" in texto2 or "OTROPAGO" in texto2 or "OTRO PAGO" in texto2:
        return "PERCEPCIONES"

    return None


def ordenar_columnas_concepto(cols):
    bases = {}

    for c in cols:
        c = str(c)
        if c.endswith(" EXENTO"):
            base = c[:-7]
            bases.setdefault(base, {})["EXENTO"] = c
        elif c.endswith(" GRAVADO"):
            base = c[:-8]
            bases.setdefault(base, {})["GRAVADO"] = c

    resultado = []
    for base in sorted(bases.keys(), key=lambda x: x.upper()):
        if "EXENTO" in bases[base]:
            resultado.append(bases[base]["EXENTO"])
        if "GRAVADO" in bases[base]:
            resultado.append(bases[base]["GRAVADO"])

    return resultado


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
        raise ValueError("No se encontraron columnas base suficientes para consolidar por empleado y período.")

    return existentes


def transformar_bloque(df_bloque, columnas_base, col_concepto_detalle, col_exento, col_gravado):
    if df_bloque.empty:
        return pd.DataFrame(columns=columnas_base + ["TOTAL_EXENTO", "TOTAL_GRAVADO"])

    # Agrupar primero para evitar duplicados exactos antes del pivot
    agrupado = (
        df_bloque.groupby(columnas_base + [col_concepto_detalle], dropna=False, as_index=False)[[col_exento, col_gravado]]
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

    resultado = pd.concat([exento_pivot, gravado_pivot], axis=1).reset_index()

    columnas_dinamicas = [c for c in resultado.columns if c not in columnas_base]
    columnas_dinamicas = ordenar_columnas_concepto(columnas_dinamicas)

    resultado = resultado[columnas_base + columnas_dinamicas]

    cols_exento = [c for c in resultado.columns if str(c).endswith(" EXENTO")]
    cols_gravado = [c for c in resultado.columns if str(c).endswith(" GRAVADO")]

    resultado["TOTAL_EXENTO"] = resultado[cols_exento].sum(axis=1) if cols_exento else 0.0
    resultado["TOTAL_GRAVADO"] = resultado[cols_gravado].sum(axis=1) if cols_gravado else 0.0

    return resultado


def transformar_hoja_nomina(archivo, nombre_hoja):
    archivo.seek(0)
    hoja_original_df = pd.read_excel(archivo, sheet_name=nombre_hoja)
    df = hoja_original_df.copy()

    df.columns = [str(c).strip() for c in df.columns]

    col_concepto = buscar_columna(df, candidatos_exactos=["CONCEPTO"])
    col_tipo_concepto = buscar_columna(df, candidatos_exactos=["Tipo de Concepto"])
    col_concepto_detalle = buscar_columna(
        df,
        candidatos_exactos=["Texto expl.CC-nómina", "Texto expl.CC-nomina"]
    )

    # En tu archivo real, la buena es "Exento", no "Exento.1"
    col_exento = buscar_columna(df, candidatos_exactos=["Exento"])
    col_gravado = buscar_columna(df, candidatos_exactos=["Gravado"])

    if not col_concepto_detalle:
        raise ValueError("No encontré la columna 'Texto expl.CC-nómina'.")
    if not col_exento:
        raise ValueError("No encontré la columna 'Exento'.")
    if not col_gravado:
        raise ValueError("No encontré la columna 'Gravado'.")

    columnas_base = obtener_columnas_base(df)

    df[col_concepto_detalle] = df[col_concepto_detalle].apply(normalizar_texto)
    df[col_exento] = pd.to_numeric(df[col_exento], errors="coerce").fillna(0.0)
    df[col_gravado] = pd.to_numeric(df[col_gravado], errors="coerce").fillna(0.0)

    df["_TIPO_SALIDA_"] = df.apply(
        lambda r: clasificar_registro(
            r[col_concepto] if col_concepto in df.columns else "",
            r[col_tipo_concepto] if col_tipo_concepto in df.columns else "",
        ),
        axis=1
    )

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
