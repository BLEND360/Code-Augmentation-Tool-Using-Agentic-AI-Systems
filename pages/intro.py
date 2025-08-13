import streamlit as st
import pandas as pd
import os


def render():
    # Load the CSVs into DataFrames
    df_orders = pd.read_csv("pages/intro_data/ca_orders.csv", nrows=10)
    df_customers = pd.read_csv("pages/intro_data/ca_customers.csv", nrows=10)
    df_order_items = pd.read_csv("pages/intro_data/ca_order_items.csv", nrows=10)
    df_product = pd.read_csv("pages/intro_data/ca_product.csv", nrows=10)
    df_sellers = pd.read_csv("pages/intro_data/ca_sellers.csv", nrows=10)
    df_order_payments = pd.read_csv("pages/intro_data/ca_order_payments.csv", nrows=10)
    df_order_reviews = pd.read_csv("pages/intro_data/ca_order_reviews.csv", nrows=10)
    df_geolocation = pd.read_csv("pages/intro_data/ca_geolocation.csv", nrows=20)

    st.title("Introduction")
    st.write(
        """
        The **Code Augmentation Tool** is an AI-powered system that converts Snowflake SQL into ANSI-compliant SQL while enhancing query performance through intelligent analysis and optimization. It generates efficient, portable SQL queries tailored for execution in Databricks environments.

        To demonstrate the capabilities of this tool, we use a publicly available eCommerce dataset that captures real-world online retail transactions. The dataset is organized into a well-structured relational schema, enabling comprehensive analysis across various aspects of the eCommerce lifecycle. The dataset comprises of multiple interrelated tables, which include **ca_customers**, **ca_orders**, **ca_order_items**, **ca_product**, **ca_sellers**, **ca_order_payments**, **ca_order_reviews** and **ca_geolocation**.
        
        """
    )

    # ERD Image
    st.image("pages/intro_data/ERD.png", caption="Relational Schema of the eCommerce Dataset", use_column_width=True)

    # Tables Data header
    st.header("Preview of Dataset Tables")
    st.subheader("ca_orders")
    st.dataframe(df_orders, height=200, hide_index=True)

    st.subheader("ca_customers")
    st.dataframe(df_customers, height=200, hide_index=True)

    st.subheader("ca_order_items")
    st.dataframe(df_order_items, height=200, hide_index=True)
 
    st.subheader("ca_product")
    st.dataframe(df_product, height=200, hide_index=True)

    st.subheader("ca_sellers")
    st.dataframe(df_sellers, height=200, hide_index=True)

    st.subheader("ca_order_payments")
    st.dataframe(df_order_payments, height=200, hide_index=True)

    st.subheader("ca_order_reviews")
    st.dataframe(df_order_reviews, height=200, hide_index=True)

    st.subheader("ca_geolocation")
    st.dataframe(df_geolocation, height=200, hide_index=True)

    # Queries
    st.header("Example Queries")
    st.write("""
Below are some examples for efficient and inefficient SQL queries demonstrating how to extract valuable insights from this dataset,
    """
    )
    st.write("**1. Counting Seller Orders that inefficiently recalculates each row **")
    st.code("""
SELECT 
    o.order_id,
    oi.seller_id,
    (SELECT COUNT(*) 
     FROM ca_order_items oi2 
     WHERE oi2.seller_id = oi.seller_id) AS seller_order_count
FROM ca_orders o
CROSS JOIN ca_order_items oi
WHERE o.order_id = oi.order_id;
""", language="sql")

    st.write("**2. Monthly revenue from orders**")
    st.code("""
SELECT DATE_TRUNC('month', o.order_purchase_timestamp) AS month,
       SUM(oi.price + oi.freight_value) AS total_revenue
FROM ca_orders o
JOIN ca_order_items oi ON o.order_id = oi.order_id
GROUP BY month
ORDER BY month;
""", language="sql")

    st.write("**3.  Get top states by total revenue (inefficient-Redundant CTEs, Cross Joins, and Over-Nesting)**")
    st.code("""
SELECT final.customer_state, final.total_revenue
FROM (
    SELECT nested.customer_state, nested.total_revenue
    FROM (
        SELECT derived.customer_state,
               SUM(derived.price + derived.freight_value) AS total_revenue
        FROM (
            SELECT c.customer_state, i.price, i.freight_value
            FROM (
                SELECT *
                FROM ca_orders
            ) o
            JOIN (
                SELECT *
                FROM ca_order_items
            ) i ON o.order_id = i.order_id
            JOIN (
                SELECT *
                FROM ca_customers
            ) c ON o.customer_id = c.customer_id
            CROSS JOIN (
                SELECT customer_state
                FROM ca_customers
            ) AS redundant_cross -- unnecessary cross join
            WHERE c.customer_state IS NOT NULL
        ) AS derived
        GROUP BY derived.customer_state
    ) AS nested
) AS final
ORDER BY final.total_revenue DESC;
""", language="sql")

    st.write("**4. Get number of customers per state**")
    st.code("""
SELECT 
    c.customer_id,
    c.customer_state,
    -- Inefficient subquery: recalculates for each row
    (SELECT COUNT(DISTINCT c2.customer_id)
     FROM ca_customers c2
     WHERE c2.customer_state = c.customer_state) AS customers_in_state
FROM ca_customers c
CROSS JOIN ca_order_items oi
WHERE c.customer_id IN (
    SELECT customer_id FROM ca_orders WHERE order_id = oi.order_id
);
""", language="sql")
    
    st.write("To explore a deeper understanding of the dataset structure and the relationships between the tables, please refer to the documentation available [here](https://blend360.atlassian.net/wiki/x/DAAbUw)")