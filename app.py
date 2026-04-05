import streamlit as st

st.set_page_config(page_title="Prueba básica", layout="centered")

st.title("Prueba de carga Streamlit")

archivo = st.file_uploader("Sube un Excel", type=["xlsx"])

if archivo is not None:
    st.success(f"Archivo cargado: {archivo.name}")
