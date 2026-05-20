import streamlit as st
import pandas as pd
import random
from faker import Faker
from datetime import datetime
import matplotlib.pyplot as plt
from streamlit_autorefresh import st_autorefresh

fake = Faker()

# Auto refresh every 3 seconds
st_autorefresh(interval=3000, key="datarefresh")

st.set_page_config(
    page_title="Real-Time E-Commerce Dashboard",
    layout="wide"
)

st.title("Real-Time E-Commerce Analytics Dashboard")
st.markdown("Live Streaming Simulation using Kafka Concepts")

products = [
    "Laptop",
    "Phone",
    "Headphones",
    "Keyboard",
    "Mouse",
    "Monitor",
    "Camera",
    "Tablet"
]

categories = {
    "Laptop": "Electronics",
    "Phone": "Electronics",
    "Headphones": "Accessories",
    "Keyboard": "Accessories",
    "Mouse": "Accessories",
    "Monitor": "Electronics",
    "Camera": "Electronics",
    "Tablet": "Electronics"
}

payment_methods = [
    "Visa",
    "Cash",
    "PayPal",
    "MasterCard"
]

# Store data
if 'data' not in st.session_state:
    st.session_state.data = pd.DataFrame()

# Generate fake data
new_rows = []

for i in range(10):

    product = random.choice(products)

    new_rows.append({
        "Order ID": random.randint(1000, 9999),
        "Customer": fake.first_name(),
        "Product": product,
        "Category": categories[product],
        "Amount": random.randint(100, 20000),
        "Payment": random.choice(payment_methods),
        "Timestamp": datetime.now().strftime("%H:%M:%S")
    })

new_df = pd.DataFrame(new_rows)

# Append data
st.session_state.data = pd.concat(
    [st.session_state.data, new_df],
    ignore_index=True
)

# Keep latest 200 rows
st.session_state.data = st.session_state.data.tail(200)

df = st.session_state.data

# KPIs
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Orders", len(df))
col2.metric("Total Revenue", f"{df['Amount'].sum()} EGP")
col3.metric("Average Order", f"{round(df['Amount'].mean(), 2)} EGP")
col4.metric("Top Product", df['Product'].mode()[0])

# Revenue by Category
st.subheader("Revenue by Category")

category_sales = df.groupby('Category')['Amount'].sum()

fig1, ax1 = plt.subplots()
category_sales.plot(kind='bar', ax=ax1)

st.pyplot(fig1)

# Top Products
st.subheader("Top Products")

product_sales = df.groupby('Product')['Amount'].sum().sort_values(ascending=False)

st.bar_chart(product_sales)

# Payment Methods
st.subheader("Payment Methods Distribution")

payment_counts = df['Payment'].value_counts()

fig2, ax2 = plt.subplots()
payment_counts.plot(kind='pie', autopct='%1.1f%%', ax=ax2)

st.pyplot(fig2)

# Live Orders Table
st.subheader("Live Orders Stream")

st.dataframe(
    df.sort_values(by='Timestamp', ascending=False),
    use_container_width=True
)