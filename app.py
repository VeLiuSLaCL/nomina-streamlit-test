import streamlit as st
import pandas as pd
from io import BytesIO
from transform_nomina import leer_hojas_excel, transformar_hoja_nomina

st.set_page_config(page_title="Transformador de Nómina", layout="wide")

st.title("Transformador de Nómina")
st.write("Sube tu archivo Excel, elige la hoja a procesar y descarga el resultado.")

archivo = st.file_uploader("Sube tu archivo Excel", type=["xlsx", "xlsm", "xls"])

if archivo is not None:
    try:
        hojas = leer_hojas_excel(archivo)
        hoja_seleccionada = st.selectbox("Selecciona la hoja a procesar", hojas)

        if st.button("Procesar archivo"):
            with st.spinner("Procesando archivo..."):
                archivo.seek(0)
                hoja_original_df, percepciones_df, deducciones_df, nombre_salida = transformar_hoja_nomina(
                    archivo,
                    hoja_seleccionada
                )

            st.success("Archivo procesado correctamente.")

            tab1, tab2, tab3 = st.tabs(["HOJA ORIGINAL", "PERCEPCIONES", "DEDUCCIONES"])

            with tab1:
                st.subheader(f"Vista previa - {hoja_seleccionada}")
                st.dataframe(hoja_original_df.head(100), use_container_width=True)

            with tab2:
                st.subheader("Vista previa - PERCEPCIONES")
                st.dataframe(percepciones_df.head(100), use_container_width=True)

            with tab3:
                st.subheader("Vista previa - DEDUCCIONES")
                st.dataframe(deducciones_df.head(100), use_container_width=True)

            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                hoja_original_df.to_excel(writer, index=False, sheet_name="ORIGINAL")
                percepciones_df.to_excel(writer, index=False, sheet_name="PERCEPCIONES")
                deducciones_df.to_excel(writer, index=False, sheet_name="DEDUCCIONES")

                workbook = writer.book
                money_format = workbook.add_format({"num_format": "#,##0.00"})
                text_format = workbook.add_format({"num_format": "@"})
                date_format = workbook.add_format({"num_format": "dd/mm/yyyy"})

                for sheet_name, df in {
                    "ORIGINAL": hoja_original_df,
                    "PERCEPCIONES": percepciones_df,
                    "DEDUCCIONES": deducciones_df,
                }.items():
                    worksheet = writer.sheets[sheet_name]

                    for col_idx, col_name in enumerate(df.columns):
                        ancho = max(len(str(col_name)), 14)

                        if (
                            str(col_name).endswith(" EXENTO")
                            or str(col_name).endswith(" GRAVADO")
                            or str(col_name) in ["TOTAL_EXENTO", "TOTAL_GRAVADO"]
                        ):
                            worksheet.set_column(col_idx, col_idx, ancho + 2, money_format)
                        elif "fecha" in str(col_name).lower():
                            worksheet.set_column(col_idx, col_idx, max(ancho + 2, 14), date_format)
                        else:
                            worksheet.set_column(col_idx, col_idx, min(ancho + 2, 28), text_format)

            st.download_button(
                label="Descargar archivo transformado",
                data=output.getvalue(),
                file_name=nombre_salida,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    except Exception as e:
        st.error(f"Error al procesar el archivo: {e}")
