from snowflake.snowpark import Session
import pandas as pd

connection_params = {
    "account": "<your_account>",
    "user": "<your_user>",
    "password": "<your_password>",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "CHURN_DB",
    "schema": "PUBLIC"
}

def create_session():
    return Session.builder.configs(connection_params).create()

def predict_single():
    session = create_session()

    age = 35
    city = "Bangalore"
    plan = "Gold"
    monthly_usage = 45.0
    complaints = 1
    payments_missed = 0
    tenure = 18

    result = session.sql(
        f"""
        SELECT predict_churn(
            {age}, '{city}', '{plan}', {monthly_usage},
            {complaints}, {payments_missed}, {tenure}
        )
        """
    ).collect()[0][0]

    print("Predicted churn:", "YES" if result==1 else "NO")

if __name__ == "__main__":
    predict_single()