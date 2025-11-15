import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import call_udf

session = Session.builder.configs(connection_params).create()

st.title("Customer Churn Prediction – Snowflake + Snowpark")

age = st.number_input("Age", 18, 80, 30)
city = st.text_input("City")
plan = st.text_input("Plan")
monthly = st.number_input("Monthly Usage", 0.0, 500.0, 50.0)
complaints = st.number_input("Complaints", 0, 10, 1)
missed = st.number_input("Payments Missed", 0, 10, 0)
tenure = st.number_input("Tenure (months)", 1, 60, 12)

if st.button("Predict"):
    result = session.sql(
        f"""
        SELECT predict_churn({age}, '{city}', '{plan}', {monthly},
                             {complaints}, {missed}, {tenure})
        """
    ).collect()[0][0]

    st.success(f"Predicted Churn: {'Yes' if result==1 else 'No'}")