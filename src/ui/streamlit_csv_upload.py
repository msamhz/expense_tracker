import streamlit as st
import os
import pandas as pd
from src.config import *

def upload_csv_files():
    # Ensure directories exist
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(PROCESSED_FOLDER, exist_ok=True)

    st.title("Bank Transaction CSV Uploader")

    uploaded_files = st.file_uploader("Upload CSV files for processing", accept_multiple_files=True, type=["csv"])

    if uploaded_files:
        for uploaded_file in uploaded_files:
            file_path = os.path.join(UPLOAD_FOLDER, uploaded_file.name)
            
            # Save file to raw data folder
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            st.success(f"File {uploaded_file.name} uploaded successfully!")
        
        st.write("Processing will begin shortly in the next pipeline...")