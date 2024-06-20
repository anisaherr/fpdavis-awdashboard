import os
import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from gtts import gTTS
import tempfile
import os

st.set_page_config(layout="wide", page_title="Adventure Works Dashboard")

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

def text_to_speech_gtts(text):
    tts = gTTS(text=text, lang='en')
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmpfile:
        tts.save(tmpfile.name)
        return tmpfile.name

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

        query_product_sales = """
        SELECT 
            dimproduct.EnglishProductName AS Product,
            SUM(factinternetsales.OrderQuantity) AS Orders,
            SUM(factinternetsales.SalesAmount) AS Revenue,
            SUM(factinternetsales.SalesAmount - factinternetsales.TotalProductCost) AS Profit
        FROM factinternetsales
        JOIN dimproduct ON factinternetsales.ProductKey = dimproduct.ProductKey
        GROUP BY dimproduct.EnglishProductName
        ORDER BY Revenue DESC
        """
        df_product_sales = pd.read_sql(query_product_sales, conn)

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

        return df_sales,df_product_sales, df_sales_by_month, df_sales_by_category, df_top_sales_by_country
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
        df_sales, df_product_sales, df_sales_by_month, df_sales_by_category, df_top_sales_by_country = load_data_overview(selected_year)

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

            st.markdown("""
                <div style='text-align: justify;'>
                    Berdasarkan analisis data penjualan Adventure Works dari tahun 2001 hingga 2004, terlihat tren peningkatan yang signifikan dalam kinerja penjualan perusahaan. Total penjualan meningkat dari 3,27 juta USD pada tahun 2001 menjadi 9,77 juta USD pada tahun 2004, yang menunjukkan pertumbuhan lebih dari tiga kali lipat dalam empat tahun. Kuantitas produk yang terjual juga meningkat secara konsisten setiap tahun, dari 1,0 ribu unit pada tahun 2001 menjadi 32,3 ribu unit pada tahun 2004. Meskipun total penjualan dan kuantitas meningkat, profit tetap stabil dengan sedikit penurunan dari 4,07 juta USD pada tahun 2003 menjadi 4,05 juta USD pada tahun 2004. Persentase keuntungan relatif stabil dengan sedikit fluktuasi, menunjukkan efisiensi operasional yang baik. Margin keuntungan tetap kuat di sekitar 40%, mencerminkan kemampuan perusahaan untuk mempertahankan profitabilitas yang tinggi meskipun ada peningkatan dalam volume penjualan. Data ini menunjukkan performa yang mengesankan dan pertumbuhan yang berkelanjutan dari Adventure Works.
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown(f"<p style='padding-top: 8px;'></p>", unsafe_allow_html=True)
            if st.button("Convert to Speech"):
                text = f"Berdasarkan analisis data penjualan Adventure Works dari tahun dua ribu satu hingga dua ribu empat, terlihat tren peningkatan yang signifikan dalam kinerja penjualan perusahaan. Total penjualan meningkat dari tiga koma dua tujuh juta USD pada tahun dua ribu satu menjadi sembilan koma tujuh tujuh juta USD pada tahun dua ribu empat, yang menunjukkan pertumbuhan lebih dari tiga kali lipat dalam empat tahun. Kuantitas produk yang terjual juga meningkat secara konsisten setiap tahun, dari satu koma nol ribu unit pada tahun dua ribu satu menjadi tiga puluh dua koma tiga ribu unit pada tahun dua ribu empat. Meskipun total penjualan dan kuantitas meningkat, profit tetap stabil dengan sedikit penurunan dari empat koma nol tujuh juta USD pada tahun dua ribu tiga menjadi empat koma nol lima juta USD pada tahun dua ribu empat. Persentase keuntungan relatif stabil dengan sedikit fluktuasi, menunjukkan efisiensi operasional yang baik. Margin keuntungan tetap kuat di sekitar empat puluh persen, mencerminkan kemampuan perusahaan untuk mempertahankan profitabilitas yang tinggi meskipun ada peningkatan dalam volume penjualan. Data ini menunjukkan performa yang mengesankan dan pertumbuhan yang berkelanjutan dari Adventure Works."
                audio_file = text_to_speech_gtts(text)
                st.audio(audio_file)
                os.remove(audio_file)
            
            col1, col2 = st.columns(2)

            with col1:
                #Grafik Sales by Month
                fig_sales_month = px.line(df_sales_by_month, x='Month', y='Sales', title='Sales by Month', markers=True, color_discrete_sequence=['#003f5c'])
                st.plotly_chart(fig_sales_month, use_container_width=True)

                #Grafik 5 Sales by Country
                fig_top_sales_country = px.bar(
                        df_top_sales_by_country, 
                        x='SalesTerritoryCountry', 
                        y='TotalSales', 
                        title='Top 5 Sales by Country', 
                        color_discrete_sequence=['#ffa600'],
                    )
                fig_top_sales_country.update_layout(xaxis={'tickangle': 0})
                st.plotly_chart(fig_top_sales_country, use_container_width=True)

                #Grafik Sales by Category
                fig_sales_category = px.pie(df_sales_by_category, names='Category', values='Sales', title='Sales by Category', color_discrete_sequence=['#003f5c','#665191','#a05195'])
                st.plotly_chart(fig_sales_category, use_container_width=True)

            with col2:
                st.markdown(f"<p style='padding-top: 10px;'></p>", unsafe_allow_html=True)
                st.subheader("Grafik Sales by Month")
                st.markdown("""
                <div style='text-align: justify;'>
                    Pada tahun 2001, penjualan mulai meningkat tajam pada bulan Desember, mencapai puncaknya di akhir tahun. Tahun 2002 menunjukkan fluktuasi yang lebih besar dengan beberapa penurunan tajam di pertengahan tahun, namun tetap menunjukkan peningkatan di bulan-bulan terakhir. Pada tahun 2003, penjualan menunjukkan tren yang terus meningkat sepanjang tahun dengan lonjakan signifikan di bulan November dan Desember. Tren ini berlanjut pada tahun 2004 dengan peningkatan penjualan yang stabil hingga mencapai puncaknya pada bulan Juni sebelum turun drastis. Data ini menunjukkan pola musiman dan potensi puncak penjualan di akhir tahun yang dapat dimanfaatkan untuk strategi penjualan dan pemasaran yang lebih efektif.
                </div>
                """, unsafe_allow_html=True)

                st.markdown(f"<p style='padding-top: 10px;'></p>", unsafe_allow_html=True)
                st.subheader("Grafik Top Sales by Country")
                st.markdown("""
                <div style='text-align: justify;'>
                    Grafik ini menunjukkan bahwa Amerika Serikat dan Australia konsisten sebagai dua negara dengan total penjualan tertinggi. Pada tahun 2001, Australia memimpin dengan total penjualan sekitar 1,2 juta, diikuti oleh Amerika Serikat. Tren ini berlanjut hingga tahun 2002 dengan peningkatan yang signifikan di Amerika Serikat, menyusul Australia di posisi kedua. Pada tahun 2003, meskipun posisi puncak dipegang oleh Australia, Amerika Serikat berhasil mendekati dengan peningkatan penjualan yang signifikan. Pada tahun 2004, Amerika Serikat mencapai puncak penjualan tertinggi dengan total lebih dari 3 juta, sementara Australia turun ke posisi kedua. Hal ini menunjukkan pertumbuhan pasar yang signifikan di Amerika Serikat, yang menjadi pasar utama Adventure Works dalam periode ini, dengan peningkatan yang konsisten setiap tahun. Adapun negara-negara seperti Inggris, Jerman, dan Perancis, meskipun berkontribusi pada penjualan, tetap berada di posisi lebih rendah dengan total penjualan lebih sedikit.
                </div>
                """, unsafe_allow_html=True)

                st.markdown(f"<p style='padding-top: 10px;'></p>", unsafe_allow_html=True)
                st.subheader("Grafik Sales by Category Products")
                st.markdown("""
                <div style='text-align: justify;'>
                    Grafik memperlihatkan bahwa sepeda (Bikes) mendominasi penjualan dengan persentase yang sangat tinggi, yaitu 100% pada tahun 2001 dan 2002, serta sedikit menurun menjadi 95,6% pada tahun 2003 dan 93,8% pada tahun 2004. Penjualan aksesoris (Accessories) dan pakaian (Clothing) mulai muncul pada tahun 2003 dan 2004, meskipun kontribusinya masih sangat kecil dibandingkan dengan penjualan sepeda. Hal ini menunjukkan bahwa Adventure Works sangat bergantung pada penjualan sepeda sebagai produk utama mereka, namun mulai menunjukkan diversifikasi produk dengan memperkenalkan aksesoris dan pakaian. 
                """, unsafe_allow_html=True)
                
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

        st.markdown("""
        <div style='text-align: justify;'>
        Analisis pelanggan Adventure Works menunjukkan bahwa total pelanggan mencapai 18.5K dengan rata-rata pendapatan per pelanggan sebesar $486.04. Pembagian gender pelanggan cukup seimbang, dengan 9351 pelanggan laki-laki dan 9133 pelanggan perempuan. Hal ini menandakan bahwa produk dan layanan Adventure Works berhasil menarik minat yang hampir sama antara kedua gender, menunjukkan inklusivitas dan daya tarik yang luas dari penawaran perusahaan.
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"<p style='padding-top: 8px;'></p>", unsafe_allow_html=True)
        if st.button("Convert to Speech"):
            text = f"Analisis pelanggan Adventure Works menunjukkan bahwa total pelanggan mencapai delapan belas ribu lima ratus dengan rata-rata pendapatan per pelanggan sebesar empat ratus delapan puluh enam dolar dan empat sen. Pembagian gender pelanggan cukup seimbang, dengan sembilan ribu tiga ratus lima puluh satu pelanggan laki-laki dan sembilan ribu seratus tiga puluh tiga pelanggan perempuan. Hal ini menandakan bahwa produk dan layanan Adventure Works berhasil menarik minat yang hampir sama antara kedua gender, menunjukkan inklusivitas dan daya tarik yang luas dari penawaran perusahaan."
            audio_file = text_to_speech_gtts(text)
                st.audio(audio_file)
                os.remove(audio_file)
            
        color_palette = ['#003f5c', '#58508d', '#bc5090', '#ff6361', '#ffa600']

        col1, col2 = st.columns(2)

        with col1:
            # Visualize profit trend by gender
            fig_profit_trend = px.line(df_profit_trend_by_gender, x='Month', y='Profit', title='Profit Trend by Gender', color='Gender', 
                                       color_discrete_sequence=['#1f77b4', '#bc5090'])
            st.plotly_chart(fig_profit_trend, use_container_width=True)

            # Visualize profit by age group
            fig_profit_age = px.bar(df_profit_by_age_group, x='AgeGroup', y='Profit', title='Profit by Age', text='Profit', color='AgeGroup', color_discrete_sequence=color_palette)
            st.plotly_chart(fig_profit_age, use_container_width=True)

            # Visualize profit by customer profession
            fig_profit_profession = px.bar(df_profit_by_profession, x='Profit', y='Profession', title='Profit by Customer Profession', color='Profession', color_discrete_sequence=color_palette)
            st.plotly_chart(fig_profit_profession, use_container_width=True)

        with col2:
            st.markdown(f"<p style='padding-top: 30px;'></p>", unsafe_allow_html=True)
            st.subheader("Grafik Profit Trend by Gender")
            st.markdown("""
                <div style='text-align: justify;'>
                    Pada bulan Januari hingga Mei, keuntungan untuk kedua gender meningkat dengan puncak sekitar bulan Juni, di mana keuntungan mencapai lebih dari 650k. Namun, setelah itu, keuntungan menurun drastis hingga mencapai titik terendah sekitar bulan Juli. Setelah penurunan tersebut, ada sedikit fluktuasi sebelum akhirnya keuntungan kembali meningkat tajam pada bulan November dan Desember, kembali mencapai sekitar 650k.
                    Penggunaan grafik garis sangat efektif dalam kasus ini karena memungkinkan untuk melihat perubahan keuntungan yang halus dan terus-menerus selama periode waktu yang ditentukan. Ini juga memudahkan perbandingan langsung antara keuntungan dari pelanggan pria dan wanita, menunjukkan bahwa keduanya cenderung memiliki pola pengeluaran yang sangat mirip. 
                </div>
                """, unsafe_allow_html=True)       
             
            st.markdown(f"<p style='padding-top: 50px;'></p>", unsafe_allow_html=True)
            st.subheader("Grafik Profit by Age")
            st.markdown("""
                <div style='text-align: justify;'>
                    Berdasarkan grafik, terlihat bahwa kelompok usia di atas 70 tahun memberikan kontribusi keuntungan terbesar, mencapai sekitar 5,6 juta. Kelompok usia 60-69 tahun berada di posisi kedua dengan keuntungan sekitar 3,7 juta, sementara kelompok usia 50-59 tahun memberikan kontribusi keuntungan paling rendah, yaitu sekitar 2,7 juta.
                    Penggunaan grafik batang memudahkan untuk melihat perbandingan keuntungan secara langsung antar kelompok usia. Diagram menunjukkan bahwa pelanggan yang lebih tua, khususnya yang berusia di atas 70 tahun, merupakan segmen pasar yang sangat penting dan paling menguntungkan bagi Adventure Works. 
                </div>
                """, unsafe_allow_html=True)  
            
            st.markdown(f"<p style='padding-top: 80px;'></p>", unsafe_allow_html=True)
            st.subheader("Profit by Customer Profession")
            st.markdown("""
                <div style='text-align: justify;'>
                     Dari grafik ini, terlihat bahwa pelanggan dengan profesi Profesional memberikan kontribusi keuntungan terbesar, mencapai sekitar 4 juta. Profesi Skilled Manual berada di posisi kedua dengan keuntungan mendekati 3 juta, sementara Management menghasilkan sekitar 2,5 juta. Pelanggan dengan profesi Clerical dan Manual memberikan kontribusi yang lebih kecil, masing-masing sekitar 1,5 juta dan 1 juta. Dari data ini, dapat disimpulkan bahwa strategi pemasaran yang menargetkan pelanggan dengan profesi Profesional dan Skilled Manual dapat lebih menguntungkan bagi Adventure Works, mengingat tingginya kontribusi keuntungan dari segmen ini. 
                </div>
                """, unsafe_allow_html=True)       

    else:
        st.error("Data not available to display.")


