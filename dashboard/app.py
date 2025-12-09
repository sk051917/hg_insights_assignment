import streamlit as st
from pyspark.sql import SparkSession
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

st.set_page_config(page_title="ETL Pipeline Dashboard", layout="wide")

@st.cache_resource
def get_spark():
    return (SparkSession.builder
        .appName("Dashboard")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.driver.memory", "1g")
        .master("local[*]")
        .getOrCreate())

spark = get_spark()

def read_delta(path):
    try:
        return spark.read.format("delta").load(path).toPandas()
    except Exception as e:
        st.error(f"Error reading {path}: {e}")
        return None
    
def read_csv(path):
    try:
        return pd.read_csv(path)
    except Exception as e:
        st.error(f"Error reading {path}: {e}")
        return None

# Paths
WAREHOUSE = "/opt/datasets/warehouse"
RAW_CUSTOMERS = f"{WAREHOUSE}/raw.db/customers"
RAW_ORDERS = f"{WAREHOUSE}/raw.db/orders"
RAW_PRODUCTS = f"{WAREHOUSE}/raw.db/products"
STRUCT_CUSTOMERS = f"{WAREHOUSE}/structured.db/customers"
STRUCT_ORDERS = f"{WAREHOUSE}/structured.db/orders"
STRUCT_PRODUCTS = f"{WAREHOUSE}/structured.db/products"
STRUCT_CUSTOMERS_REJ = f"{WAREHOUSE}/structured.db/customers_rejected"
STRUCT_ORDERS_REJ = f"{WAREHOUSE}/structured.db/orders_rejected"
STRUCT_PRODUCTS_REJ = f"{WAREHOUSE}/structured.db/products_rejected"

COUNTRY_CODES = "/opt/datasets/static_data/country_codes.csv"
DEPARTMENT_CODES = "/opt/datasets/static_data/department_codes.csv"

st.title("📊 ETL Pipeline Dashboard")

tab1, tab2, tab3 = st.tabs(["📥 Data Ingestion", "🔍 Data Quality", "📈 Analytics"])

with tab1:
    st.header("Data Ingestion Summary")
    
    col1, col2, col3 = st.columns(3)
    
    raw_cust = read_delta(RAW_CUSTOMERS)
    raw_ord = read_delta(RAW_ORDERS)
    raw_prod = read_delta(RAW_PRODUCTS)
    
    struct_cust = read_delta(STRUCT_CUSTOMERS)
    struct_ord = read_delta(STRUCT_ORDERS)
    struct_prod = read_delta(STRUCT_PRODUCTS)

    rej_cust = read_delta(STRUCT_CUSTOMERS_REJ)
    rej_ord = read_delta(STRUCT_ORDERS_REJ)
    rej_prod = read_delta(STRUCT_PRODUCTS_REJ)
    
    with col1:
        st.subheader("Customers")
        st.metric("Raw", len(raw_cust) if raw_cust is not None else 0)
        st.metric("Structured", len(struct_cust) if struct_cust is not None else 0)
        st.metric("Rejected", len(rej_cust) if rej_cust is not None else 0)
    
    with col2:
        st.subheader("Orders")
        st.metric("Raw", len(raw_ord) if raw_ord is not None else 0)
        st.metric("Structured", len(struct_ord) if struct_ord is not None else 0)
        st.metric("Rejected", len(rej_ord) if rej_ord is not None else 0)

    with col3:
        st.subheader("Products")
        st.metric("Raw", len(raw_prod) if raw_prod is not None else 0)
        st.metric("Structured", len(struct_prod) if struct_prod is not None else 0)
        st.metric("Rejected", len(rej_prod) if rej_prod is not None else 0)
    
    st.divider()
    
    # Pipeline Execution Metrics
    st.subheader("Pipeline Execution Metrics")
    
    col_a, col_b, col_c, col_d = st.columns(4)
    
    # Total runs based on unique insertion timestamps
    with col_a:
        if raw_cust is not None and 'insertion_timestamp' in raw_cust.columns:
            total_runs = raw_cust['insertion_timestamp'].nunique()
            st.metric("🔄 Total Pipeline Runs", total_runs)
        else:
            st.metric("🔄 Total Pipeline Runs", "N/A")
    
    # Last run timestamp
    with col_b:
        if raw_cust is not None and 'insertion_timestamp' in raw_cust.columns:
            last_run = raw_cust['insertion_timestamp'].max()
            st.metric("⏰ Last Run", last_run.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(last_run) else "N/A")
        else:
            st.metric("⏰ Last Run", "N/A")
    
    # Overall data quality rate
    with col_c:
        total_raw = (len(raw_cust) if raw_cust is not None else 0) + \
                   (len(raw_ord) if raw_ord is not None else 0) + \
                   (len(raw_prod) if raw_prod is not None else 0)
        
        total_structured = (len(struct_cust) if struct_cust is not None else 0) + \
                          (len(struct_ord) if struct_ord is not None else 0) + \
                          (len(struct_prod) if struct_prod is not None else 0)
        
        quality_rate = (total_structured / total_raw * 100) if total_raw > 0 else 0
        st.metric("✅ Data Quality Rate", f"{quality_rate:.1f}%")
    

# TAB 2: Data Quality
with tab2:
    st.header("Data Quality Metrics")
    
    rej_cust = read_delta(STRUCT_CUSTOMERS_REJ)
    rej_ord = read_delta(STRUCT_ORDERS_REJ)
    rej_prod = read_delta(STRUCT_PRODUCTS_REJ)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Customers")
        valid = len(struct_cust) if struct_cust is not None else 0
        rejected = len(rej_cust) if rej_cust is not None else 0
        total = valid + rejected
        rate = (rejected / total * 100) if total > 0 else 0
        st.metric("Valid", valid)
        st.metric("Rejected", rejected)
        st.metric("Rejection Rate", f"{rate:.2f}%")
    
    with col2:
        st.subheader("Orders")
        valid = len(struct_ord) if struct_ord is not None else 0
        rejected = len(rej_ord) if rej_ord is not None else 0
        total = valid + rejected
        rate = (rejected / total * 100) if total > 0 else 0
        st.metric("Valid", valid)
        st.metric("Rejected", rejected)
        st.metric("Rejection Rate", f"{rate:.2f}%")
    
    with col3:
        st.subheader("Products")
        valid = len(struct_prod) if struct_prod is not None else 0
        rejected = len(rej_prod) if rej_prod is not None else 0
        total = valid + rejected
        rate = (rejected / total * 100) if total > 0 else 0
        st.metric("Valid", valid)
        st.metric("Rejected", rejected)
        st.metric("Rejection Rate", f"{rate:.2f}%")
    
    st.divider()
    
    # Rejection Reasons - Pie Charts
    st.subheader("Rejection Reasons")
    
    col_a, col_b, col_c = st.columns(3)
    
    with col_a:
        if rej_cust is not None and 'rejection_reason' in rej_cust.columns and len(rej_cust) > 0:
            reason_counts = rej_cust['rejection_reason'].value_counts().reset_index()
            reason_counts.columns = ['reason', 'count']
            
            fig1 = px.pie(reason_counts, 
                         values='count', 
                         names='reason',
                         title='Customer Rejections',
                         hole=0.3)
            fig1.update_traces(textposition='inside', textinfo='value')
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("No customer rejections")
    
    with col_b:
        if rej_ord is not None and 'rejection_reason' in rej_ord.columns and len(rej_ord) > 0:
            reason_counts = rej_ord['rejection_reason'].value_counts().reset_index()
            reason_counts.columns = ['reason', 'count']
            
            fig2 = px.pie(reason_counts, 
                         values='count', 
                         names='reason',
                         title='Order Rejections',
                         hole=0.3)
            fig2.update_traces(textposition='inside', textinfo='value')
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No order rejections")
    
    with col_c:
        if rej_prod is not None and 'rejection_reason' in rej_prod.columns and len(rej_prod) > 0:
            reason_counts = rej_prod['rejection_reason'].value_counts().reset_index()
            reason_counts.columns = ['reason', 'count']
            
            fig3 = px.pie(reason_counts, 
                         values='count', 
                         names='reason',
                         title='Product Rejections',
                         hole=0.3)
            fig3.update_traces(textposition='inside', textinfo='value')
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No product rejections")
        

# TAB 3: Analytics
with tab3:
    st.header("Business Analytics")
    
    # Load data
    customers_df = read_delta(STRUCT_CUSTOMERS)
    orders_df = read_delta(STRUCT_ORDERS)
    products_df = read_delta(STRUCT_PRODUCTS)
    country_codes_df = read_csv(COUNTRY_CODES)
    department_codes_df = read_csv(DEPARTMENT_CODES)
    
    if customers_df is not None and orders_df is not None and products_df is not None:
        # KPI Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            unique_orders = orders_df['order_id'].nunique()
            st.metric("📦 Unique Orders", f"{unique_orders:,}")
        
        with col2:
            unique_customers = customers_df['customer_id'].nunique()
            st.metric("👥 Unique Customers", f"{unique_customers:,}")
        
        with col3:
            unique_products = products_df['product_id'].nunique()
            st.metric("🛍️ Unique Products", f"{unique_products:,}")
        
        with col4:
            # Calculate total sales
            orders_with_products = orders_df.merge(products_df[['product_id', 'price']], on='product_id', how='left')
            orders_with_products['total_amount'] = orders_with_products['quantity'] * orders_with_products['price']
            total_sales = orders_with_products['total_amount'].sum()
            st.metric("💰 Total Sales", f"${total_sales:,.2f}")
        
        st.divider()
        
        # Sales by Country
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader("Sales by Country")
            if country_codes_df is not None:
                # Join orders with customers to get country_code
                orders_with_country = orders_with_products.merge(
                    customers_df[['customer_id', 'country_code']], 
                    on='customer_id', 
                    how='left'
                )
                
                # Group by country and calculate sales
                sales_by_country = orders_with_country.groupby('country_code')['total_amount'].sum().reset_index()
                sales_by_country.columns = ['country_code', 'sales']
                
                # Merge with country names
                sales_by_country = sales_by_country.merge(country_codes_df, on='country_code', how='left')
                sales_by_country = sales_by_country.sort_values('sales', ascending=False)
                
                fig_country = px.bar(sales_by_country.head(10), 
                                    x='sales', 
                                    y='country_name',
                                    orientation='h',
                                    title='Top 10 Countries by Sales',
                                    labels={'sales': 'Total Sales ($)', 'country_name': 'Country'},
                                    text='sales')
                fig_country.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
                fig_country.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_country, use_container_width=True)
            else:
                st.warning("Country codes file not available")
        
        with col_right:
            st.subheader("Customers by Department")
            if department_codes_df is not None:
                # Group customers by department
                customers_by_dept = customers_df.groupby('department_code').size().reset_index(name='count')
                
                # Merge with department names
                customers_by_dept = customers_by_dept.merge(department_codes_df, on='department_code', how='left')
                customers_by_dept = customers_by_dept.sort_values('count', ascending=False)
                
                fig_dept = px.pie(customers_by_dept, 
                                 values='count', 
                                 names='department_name',
                                 title='Customer Distribution by Department',
                                 hole=0.4)
                fig_dept.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_dept, use_container_width=True)
            else:
                st.warning("Department codes file not available")
        
        st.divider()
        
        # Sales by Region (Grouping countries into regions)
        st.subheader("Sales by Region")
        if country_codes_df is not None:
            # Define region mapping
            region_mapping = {
                'US': 'North America', 'CA': 'North America', 'MX': 'North America',
                'FR': 'Europe', 'DE': 'Europe', 'GB': 'Europe', 'IT': 'Europe', 'ES': 'Europe',
                'NL': 'Europe', 'SE': 'Europe', 'CH': 'Europe', 'BE': 'Europe', 'NO': 'Europe',
                'DK': 'Europe', 'FI': 'Europe', 'IE': 'Europe', 'PT': 'Europe', 'PL': 'Europe',
                'GR': 'Europe', 'TR': 'Europe',
                'IN': 'Asia', 'JP': 'Asia', 'CN': 'Asia', 'KR': 'Asia', 'SG': 'Asia',
                'TH': 'Asia', 'MY': 'Asia', 'PH': 'Asia', 'IL': 'Asia',
                'AU': 'Oceania', 'NZ': 'Oceania',
                'BR': 'South America', 'AR': 'South America',
                'ZA': 'Africa', 'EG': 'Africa',
                'AE': 'Middle East', 'SA': 'Middle East',
                'RU': 'Eastern Europe'
            }
            
            orders_with_country['region'] = orders_with_country['country_code'].map(region_mapping)
            sales_by_region = orders_with_country.groupby('region')['total_amount'].sum().reset_index()
            sales_by_region.columns = ['region', 'sales']
            sales_by_region = sales_by_region.sort_values('sales', ascending=False)
            
            fig_region = px.bar(sales_by_region, 
                               x='region', 
                               y='sales',
                               title='Sales by Region',
                               labels={'sales': 'Total Sales ($)', 'region': 'Region'},
                               text='sales',
                               color='sales',
                               color_continuous_scale='Blues')
            fig_region.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
            st.plotly_chart(fig_region, use_container_width=True)
    else:
        st.warning("Required data tables are not available")