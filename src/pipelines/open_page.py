import streamlit as st
import subprocess
import runpy
from src.ui.streamlit_csv_upload import upload_csv_files

def main():
    st.set_page_config(page_title="Bank Transaction CSV Uploader", layout="wide")
    upload_csv_files()
