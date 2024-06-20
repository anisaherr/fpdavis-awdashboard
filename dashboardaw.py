import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px

# Configure Streamlit page layout
st.set_page_config(layout="wide", page_title="Adventure Works Dashboard")

# Function to create a MySQL database connection
def create_connection():
    try:
        host = st.secrets["DB_HOST"]
        port = st.secrets["DB_PORT"]
        user = st.secrets["DB_USER"]
        password = st.secrets["DB_PASSWORD"]
        database = st.secrets["DB_DATABASE"]

        if None in (host, port, user, password, database):
            raise ValueError("One or more required environment variables are not set.")

        return mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
    except KeyError as e:
        st.error(f"Key error: {e}")
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")

# Example usage
try:
    conn = create_connection()
    if conn and conn.is_connected():
        st.write("Connected to MySQL database successfully!")
        # Perform operations with the connection
except mysql.connector.Error as err:
    st.write(f"Error: {err}")
finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()
