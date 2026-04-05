import pandas as pd


def normalizar_texto(valor):
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def detectar_columna(df, posibles_nombres=None, indice_fallback=None):
    if posibles_nombres is None:
        posibles_nombres = []

    columnas_normalizadas = {str(col).strip().lower(): col for col in df.columns}

    for nombre in posibles_nombres:
        nombre_lower = nombre.strip().lower()
        if nombre_lower in columnas_normalizadas:
            return columnas_normalizadas[nombre_lower]

    if indice_fallback is not None:
        return df.columns[indice_fallback]

    raise ValueError(f"No se encontró ninguna de las columnas esperadas: {posibles_nombres}")


def clasificar_tipo_concepto(valor):
    texto = normalizar_texto(valor).upper()

    if "PERCEP" in texto:
        return "PERCEPCIONES"
    if "DEDUC" in texto:
        return "DEDUCCIONES"

    return None


def preparar_dataframe_base(df):
    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]

    # Se intenta detectar por nombre; si no existe, se usa fallback por posición.
    col_tipo = detectar_columna(
        df,
        posibles_nombres=["CONCEPTO"],
        indice_fallback=13,  # N
    )
    col_texto_expl = detectar_columna(
        df,
        posibles_nombres=["Texto expl.CC-nómina", "Texto expl.CC-nomina"],
        indice_fallback=18,  # S
    )
    col_exento = detectar_columna(
        df,
        posibles_nombres=["Importe exento", "Exento"],
        indice_fallback=20,  # U
    )
    col_gravado = detectar_columna(
        df,
        posibles_nombres=["Importe gravado", "Gravado"],
        indice_fallback=21,  # V
    )

    # Base recomendada: A:M
    columnas_base = list(df.columns[:13])

    # Limpiar / normalizar
    df[col_texto_expl] = df[col_texto_expl].apply(normalizar_texto)
    df[col_exento] = pd.to_numeric(df[col_exento], errors="coerce").fillna(0.0)
    df[col_gravado] = pd.to_numeric(df[col_gravado], errors="coerce").fillna(0.0)

    df["_TIPO_SALIDA_"] = df[col_tipo].apply(clasificar_tipo_concepto)

    # Solo registros válidos
    df = df[df["_TIPO_SALIDA_"].notna()].copy()

    # Quitar conceptos vacíos
    df = df[df[col_texto_expl].str.strip() != ""].copy()

    return df, columnas_base, col_texto_expl, col_exento, col_gravado


def ordenar_columnas_concepto(columnas):
    pares = {}
    otras = []

    for col in columnas:
        if col.endswith(" EXENTO"):
            base = col[:-7]
            pares.setdefault(base, {})["EXENTO"] = col
        elif col.endswith(" GRAVADO"):
            base = col[:-8]
            pares.setdefault(base, {})["GRAVADO"] = col
        else:
            otras.append(col)

    ordenadas = []
    for base in sorted(pares.keys(), key=lambda x: x.upper()):
        if "EXENTO" in pares[base]:
            ordenadas.append(pares[base]["EXENTO"])
        if "GRAVADO" in pares[base]:
            ordenadas.append(pares[base]["GRAVADO"])

    return otras + ordenadas


def transformar_bloque(df_bloque, columnas_base, col_texto_expl, col_exento, col_gravado):
    if df_bloque.empty:
        columnas_finales = columnas_base + ["TOTAL_EXENTO", "TOTAL_GRAVADO"]
        return pd.DataFrame(columns=columnas_finales)

    pivot_exento = df_bloque.pivot_table(
        index=columnas_base,
        columns=col_texto_expl,
        values=col_exento,
        aggfunc="sum",
        fill_value=0,
    )

    pivot_gravado = df_bloque.pivot_table(
        index=columnas_base,
        columns=col_texto_expl,
        values=col_gravado,
        aggfunc="sum",
        fill_value=0,
    )

    pivot_exento.columns = [f"{str(c).strip()} EXENTO" for c in pivot_exento.columns]
    pivot_gravado.columns = [f"{str(c).strip()} GRAVADO" for c in pivot_gravado.columns]

    resultado = pd.concat([pivot_exento, pivot_gravado], axis=1).reset_index()

    columnas_concepto = [c for c in resultado.columns if c not in columnas_base]
    columnas_ordenadas = ordenar_columnas_concepto(columnas_concepto)

    resultado = resultado[columnas_base + columnas_ordenadas]

    cols_exento = [c for c in resultado.columns if c.endswith(" EXENTO")]
    cols_gravado = [c for c in resultado.columns if c.endswith(" GRAVADO")]

    resultado["TOTAL_EXENTO"] = resultado[cols_exento].sum(axis=1) if cols_exento else 0.0
    resultado["TOTAL_GRAVADO"] = resultado[cols_gravado].sum(axis=1) if cols_gravado else 0.0

    return resultado


def transformar_archivo_excel(archivo):
    xls = pd.ExcelFile(archivo)

    nombre_hoja = None
    for hoja in xls.sheet_names:
        if str(hoja).strip().upper() == "NOM MAR":
            nombre_hoja = hoja
            break

    if nombre_hoja is None:
        raise ValueError("No se encontró la hoja 'NOM MAR' en el archivo.")

    df = pd.read_excel(archivo, sheet_name=nombre_hoja)

    df, columnas_base, col_texto_expl, col_exento, col_gravado = preparar_dataframe_base(df)

    percepciones = df[df["_TIPO_SALIDA_"] == "PERCEPCIONES"].copy()
    deducciones = df[df["_TIPO_SALIDA_"] == "DEDUCCIONES"].copy()

    percepciones_final = transformar_bloque(
        percepciones,
        columnas_base,
        col_texto_expl,
        col_exento,
        col_gravado,
    )

    deducciones_final = transformar_bloque(
        deducciones,
        columnas_base,
        col_texto_expl,
        col_exento,
        col_gravado,
    )

    nombre_salida = "nomina_transformada.xlsx"
    return percepciones_final, deducciones_final, nombre_salida
