import os
import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv

st.set_page_config(layout="wide", page_title="Adventure Works Dashboard")

# Function to create a MySQL database connection
def create_connection():
    try:
        conn = mysql.connector.connect(
            host=st.secrets["DB_HOST"],
            port=st.secrets["DB_PORT"],
            user=st.secrets["DB_USER"],
            password=st.secrets["DB_PASSWORD"],
            database=st.secrets["DB_DATABASE"]
        )
        return conn
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return None

# Function to fetch data from the database
def load_data_overview(year):
    conn = create_connection()
    if conn is None:
        return None, None, None, None 

    try:
        query_sales = f"""
        SELECT 
            SUM(SalesAmount) AS TotalSales, 
            SUM(OrderQuantity) AS TotalQuantity,
            SUM(SalesAmount - TotalProductCost) AS Profit,
            SUM(SalesAmount - TotalProductCost) / SUM(SalesAmount) * 100 AS ProfitPercentage
        FROM factinternetsales
        JOIN dimtime ON factinternetsales.OrderDateKey = dimtime.TimeKey
        WHERE dimtime.CalendarYear = {year}
        """
        df_sales = pd.read_sql(query_sales, conn)

        query_sales_by_month = f"""
        SELECT 
            MONTH(dimtime.FullDateAlternateKey) AS Month,
            SUM(factinternetsales.SalesAmount) AS Sales,
            SUM(factinternetsales.OrderQuantity) AS Quantity
        FROM factinternetsales
        JOIN dimtime ON factinternetsales.OrderDateKey = dimtime.TimeKey
        WHERE dimtime.CalendarYear = {year}
        GROUP BY MONTH(dimtime.FullDateAlternateKey)
        """
        df_sales_by_month = pd.read_sql(query_sales_by_month, conn)

        query_sales_by_category = f"""
        SELECT 
            dimproductcategory.EnglishProductCategoryName AS Category,
            SUM(factinternetsales.SalesAmount) AS Sales
        FROM factinternetsales
        JOIN dimproduct ON factinternetsales.ProductKey = dimproduct.ProductKey
        JOIN dimproductsubcategory ON dimproduct.ProductSubcategoryKey = dimproductsubcategory.ProductSubcategoryKey
        JOIN dimproductcategory ON dimproductsubcategory.ProductCategoryKey = dimproductcategory.ProductCategoryKey
        JOIN dimtime ON factinternetsales.OrderDateKey = dimtime.TimeKey
        WHERE dimtime.CalendarYear = {year}
        GROUP BY dimproductcategory.EnglishProductCategoryName
        """
        df_sales_by_category = pd.read_sql(query_sales_by_category, conn)

        query_top_sales_by_country = f"""
        SELECT 
            SalesTerritoryCountry,
            SUM(SalesAmount) AS TotalSales
        FROM factinternetsales
        JOIN dimsalesterritory ON factinternetsales.SalesTerritoryKey = dimsalesterritory.SalesTerritoryKey
        JOIN dimtime ON factinternetsales.OrderDateKey = dimtime.TimeKey
        WHERE dimtime.CalendarYear = {year}
        GROUP BY SalesTerritoryCountry
        ORDER BY TotalSales DESC
        LIMIT 5
        """
        df_top_sales_by_country = pd.read_sql(query_top_sales_by_country, conn)

        country_mapping = {
            'United States': 'USA',
            'United Kingdom': 'UK'
        }

        df_top_sales_by_country['SalesTerritoryCountry'] = df_top_sales_by_country['SalesTerritoryCountry'].replace(country_mapping)

        return df_sales, df_sales_by_month, df_sales_by_category, df_top_sales_by_country
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return None, None, None, None
    finally:
        conn.close()

def load_data_customer():
    conn = create_connection()
    if conn is None:
        return None

    try:
        query_total_customers = """
        SELECT COUNT(*) AS TotalCustomers
        FROM dimcustomer
        """
        df_total_customers = pd.read_sql(query_total_customers, conn)

        query_gender_distribution = """
        SELECT Gender, COUNT(*) AS Count 
        FROM dimcustomer 
        GROUP BY Gender
        """
        df_gender_distribution = pd.read_sql(query_gender_distribution, conn)

        query_average_revenue = """
        SELECT AVG(SalesAmount) AS AverageRevenuePerCustomer
        FROM factinternetsales
        """
        df_average_revenue = pd.read_sql(query_average_revenue, conn)

        query_profit_trend_by_gender = """
        SELECT MONTH(dimtime.FullDateAlternateKey) AS Month, Gender, SUM(SalesAmount - TotalProductCost) AS Profit
        FROM factinternetsales
        JOIN dimcustomer ON factinternetsales.CustomerKey = dimcustomer.CustomerKey
        JOIN dimtime ON factinternetsales.OrderDateKey = dimtime.TimeKey
        GROUP BY MONTH(dimtime.FullDateAlternateKey), Gender
        ORDER BY Month
        """
        df_profit_trend_by_gender = pd.read_sql(query_profit_trend_by_gender, conn)

        query_profit_by_age_group = """
        SELECT AgeGroup, SUM(SalesAmount - TotalProductCost) AS Profit
        FROM (
            SELECT CASE 
                WHEN TIMESTAMPDIFF(YEAR, BirthDate, CURDATE()) < 30 THEN '< 30 Years'
                WHEN TIMESTAMPDIFF(YEAR, BirthDate, CURDATE()) BETWEEN 30 AND 39 THEN '30 - 39 Years'
                WHEN TIMESTAMPDIFF(YEAR, BirthDate, CURDATE()) BETWEEN 40 AND 49 THEN '40 - 49 Years'
                WHEN TIMESTAMPDIFF(YEAR, BirthDate, CURDATE()) BETWEEN 50 AND 59 THEN '50 - 59 Years'
                WHEN TIMESTAMPDIFF(YEAR, BirthDate, CURDATE()) BETWEEN 60 AND 69 THEN '60 - 69 Years'
                ELSE '> 70 Years'
            END AS AgeGroup,
            SalesAmount, TotalProductCost
            FROM factinternetsales
            JOIN dimcustomer ON factinternetsales.CustomerKey = dimcustomer.CustomerKey
        ) AS AgeData
        GROUP BY AgeGroup
        """
        df_profit_by_age_group = pd.read_sql(query_profit_by_age_group, conn)

        query_profit_by_profession = """
        SELECT EnglishOccupation AS Profession, SUM(SalesAmount - TotalProductCost) AS Profit
        FROM factinternetsales
        JOIN dimcustomer ON factinternetsales.CustomerKey = dimcustomer.CustomerKey
        GROUP BY EnglishOccupation
        """
        df_profit_by_profession = pd.read_sql(query_profit_by_profession, conn)

        return df_total_customers, df_average_revenue, df_gender_distribution, df_profit_trend_by_gender, df_profit_by_age_group, df_profit_by_profession
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return None
    finally:
        conn.close()

def format_number(number):
    if isinstance(number, str):
        number = float(number.replace(',', ''))
    if number >= 10**9:
        return f"{number / 10**9:.2f}B"
    elif number >= 10**6:
        return f"{number / 10**6:.2f}M"
    elif number >= 10**3:
        return f"{number / 10**3:.1f}K"
    else:
        return f"{number:.2f}"

@st.cache_data
def load_years():
    conn = create_connection()
    if conn is None:
        return []

    try:
        query_years = """
        SELECT DISTINCT CalendarYear
        FROM dimtime
        WHERE
        CalendarYear BETWEEN 2001 AND 2004
        """
        df_years = pd.read_sql(query_years, conn)
        years = df_years['CalendarYear'].tolist()
        years = sorted(years, reverse=True) 
        return years
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return []
    finally:
        conn.close()

years = load_years()

# Sidebar
with st.sidebar:
    st.image('logoaw.png', use_column_width=True)
    page = st.selectbox("Select Page", ["Sales Overview", "Customer Analysis"])

if page == "Sales Overview":
    selected_year = st.sidebar.selectbox("Select Year", options=years)

    if selected_year:
        df_sales, df_sales_by_month, df_sales_by_category, df_top_sales_by_country = load_data_overview(selected_year)

        if df_sales is not None:
            st.title(f"Sales Overview - {selected_year}")
            total_sales = format_number(df_sales['TotalSales'].values[0])
            total_quantity = format_number(df_sales['TotalQuantity'].values[0])
            profit = format_number(df_sales['Profit'].values[0])
            profit_percentage = df_sales['ProfitPercentage'].values[0]

            col1, col2, col3, col4 = st.columns(4)
            col1.metric(label="Total Sales", value=total_sales)
            col2.metric(label="Total Quantity", value=total_quantity)
            col3.metric(label="Profit", value=profit)
            col4.metric(label="Profit %", value=f"{profit_percentage:.1f}%")

            col1, col2 = st.columns(2)

            with col1:
                fig_sales_month = px.line(df_sales_by_month, x='Month', y='Sales', title='Sales by Month', markers=True, color_discrete_sequence=['#003f5c'])
                st.plotly_chart(fig_sales_month, use_container_width=True)

                fig_sales_category = px.pie(df_sales_by_category, names='Category', values='Sales', title='Sales by Category', color_discrete_sequence=['#003f5c','#665191','#a05195'])
                st.plotly_chart(fig_sales_category, use_container_width=True)

            with col2:
                if not df_top_sales_by_country.empty:
                    fig_top_sales_country = px.bar(
                        df_top_sales_by_country, 
                        x='SalesTerritoryCountry', 
                        y='TotalSales', 
                        title='Top 5 Sales by Country', 
                        color_discrete_sequence=['#ffa600'],
                    )
                    fig_top_sales_country.update_layout(xaxis={'tickangle': 0})
                    st.plotly_chart(fig_top_sales_country, use_container_width=True)
                else:
                    st.warning("No data available for top 5 sales by country.")

        else:
            st.error("Data not available for the selected year.")

    else:
        st.error("No year selected.")

elif page == "Customer Analysis":
    df_total_customers, df_average_revenue, df_gender_distribution, df_profit_trend_by_gender, df_profit_by_age_group, df_profit_by_profession = load_data_customer()

    if df_total_customers is not None and df_average_revenue is not None:
        st.title("Customer Analysis")

        total_customers = format_number(df_total_customers['TotalCustomers'].values[0])
        average_revenue_per_customer = format_number(df_average_revenue['AverageRevenuePerCustomer'].values[0])
        total_male_customers = df_gender_distribution[df_gender_distribution['Gender'] == 'M']['Count'].values[0]
        total_female_customers = df_gender_distribution[df_gender_distribution['Gender'] == 'F']['Count'].values[0]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric(label="Total Customers", value=total_customers)
        col2.metric(label="Avg Revenue per Customer", value=average_revenue_per_customer)
        col3.metric(label="Male Customers", value=total_male_customers)
        col4.metric(label="Female Customers", value=total_female_customers)
        color_palette = ['#003f5c', '#58508d', '#bc5090', '#ff6361', '#ffa600']

        col1, col2 = st.columns(2)

        with col1:
            # Visualize gender distribution of customers
            fig_gender = px.pie(df_gender_distribution, names='Gender', values='Count', title='Customer Gender', hole=0.3,
                                color_discrete_sequence=['#1f77b4', '#bc5090'])
            st.plotly_chart(fig_gender, use_container_width=True)

            # Visualize profit by age group
            fig_profit_age = px.bar(df_profit_by_age_group, x='AgeGroup', y='Profit', title='Profit by Age', text='Profit', color='AgeGroup', color_discrete_sequence=color_palette)
            st.plotly_chart(fig_profit_age, use_container_width=True)

        with col2:
            # Visualize profit trend by gender
            fig_profit_trend = px.line(df_profit_trend_by_gender, x='Month', y='Profit', title='Profit Trend by Gender', color='Gender', 
                                       color_discrete_sequence=['#1f77b4', '#bc5090'])
            st.plotly_chart(fig_profit_trend, use_container_width=True)

            # Visualize profit by customer profession
            fig_profit_profession = px.bar(df_profit_by_profession, x='Profit', y='Profession', title='Profit by Customer Profession', color='Profession', color_discrete_sequence=color_palette)
            st.plotly_chart(fig_profit_profession, use_container_width=True)

    else:
        st.error("Data not available to display.")


