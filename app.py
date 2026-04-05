import streamlit as st
import pandas as pd
from io import BytesIO
from transform_nomina import transformar_archivo_excel

st.set_page_config(page_title="Transformador de Nómina", layout="wide")

st.title("Transformador de Nómina")
st.write(
    "Sube tu archivo Excel. Se procesará la hoja 'NOM MAR' y se generará un archivo "
    "con las hojas PERCEPCIONES y DEDUCCIONES."
)

archivo = st.file_uploader("Sube tu archivo Excel", type=["xlsx", "xlsm", "xls"])

if archivo is not None:
    try:
        with st.spinner("Procesando archivo..."):
            percepciones_df, deducciones_df, nombre_salida = transformar_archivo_excel(archivo)

            st.success("Archivo procesado correctamente.")

            tab1, tab2 = st.tabs(["PERCEPCIONES", "DEDUCCIONES"])

            with tab1:
                st.subheader("Vista previa - PERCEPCIONES")
                st.dataframe(percepciones_df, use_container_width=True)

            with tab2:
                st.subheader("Vista previa - DEDUCCIONES")
                st.dataframe(deducciones_df, use_container_width=True)

            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                percepciones_df.to_excel(writer, index=False, sheet_name="PERCEPCIONES")
                deducciones_df.to_excel(writer, index=False, sheet_name="DEDUCCIONES")

                workbook = writer.book

                money_format = workbook.add_format({"num_format": "#,##0.00"})
                text_format = workbook.add_format({"num_format": "@"})

                for sheet_name, df in {
                    "PERCEPCIONES": percepciones_df,
                    "DEDUCCIONES": deducciones_df,
                }.items():
                    worksheet = writer.sheets[sheet_name]

                    for col_idx, col_name in enumerate(df.columns):
                        max_len = max(len(str(col_name)), 14)
                        if col_name.endswith("EXENTO") or col_name.endswith("GRAVADO") or col_name in ["TOTAL_EXENTO", "TOTAL_GRAVADO"]:
                            worksheet.set_column(col_idx, col_idx, max_len + 2, money_format)
                        else:
                            worksheet.set_column(col_idx, col_idx, min(max_len + 2, 28), text_format)

            st.download_button(
                label="Descargar archivo transformado",
                data=output.getvalue(),
                file_name=nombre_salida,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    except Exception as e:
        st.error(f"Error al procesar el archivo: {e}")
