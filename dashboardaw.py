import os
import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
import toml

# Configure Streamlit page layout
st.set_page_config(layout="wide", page_title="Adventure Works Dashboard")

# Load secrets from the TOML file
def load_secrets():
    with open('secrets.toml', 'r') as f:
        return toml.load(f)

secrets = load_secrets()
db_config = secrets['database']

# Function to create a MySQL database connection
def create_connection():
    host = db_config.get('DB_HOST')
    port = db_config.get('DB_PORT')
    user = db_config.get('DB_USER')
    password = db_config.get('DB_PASSWORD')
    database = db_config.get('DB_DATABASE')

    # Debugging print statements to check environment variables
    st.write(f"DB_HOST: {host}")
    st.write(f"DB_PORT: {port}")
    st.write(f"DB_USER: {user}")
    st.write(f"DB_PASSWORD: {password}")
    st.write(f"DB_DATABASE: {database}")

    if None in (host, port, user, password, database):
        raise ValueError("One or more required environment variables are not set.")

    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )

# Example usage
try:
    conn = create_connection()
    if conn.is_connected():
        st.write("Connected to MySQL database successfully!")
    # Perform operations with the connection
except mysql.connector.Error as err:
    st.write(f"Error: {err}")
finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()
