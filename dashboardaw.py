import os
import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv

# Configure Streamlit page layout
st.set_page_config(layout="wide", page_title="Adventure Works Dashboard")

# Load environment variables from .env file
load_dotenv()

# Function to create a MySQL database connection
def create_connection():
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    database = os.getenv('DB_DATABASE')

    # Debugging print statements to check environment variables
    st.write(f"DB_HOST: {host}")
    st.write(f"DB_PORT: {port}")
    st.write(f"DB_USER: {user}")
    st.write(f"DB_PASSWORD: {password}")
    st.write(f"DB_DATABASE: {database}")

    # Ensure port is converted to an integer
    try:
        port = int(port)
    except (TypeError, ValueError):
        raise ValueError("DB_PORT environment variable must be a valid integer")

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
        print("Connected to MySQL database successfully!")
    # Perform operations with the connection
except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()
