from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

def create_session():
    connection_params = {
        "account": "<your_account>",
        "user": "<your_user>",
        "password": "<your_password>",
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": "CHURN_DB",
        "schema": "PUBLIC"
    }
    return Session.builder.configs(connection_params).create()

def main():
    session = create_session()
    df = session.table("CUSTOMER_CHURN")

    print("\n======== ROW COUNT ========")
    print(df.count())

    print("\n======== SAMPLE DATA ========")
    print(df.limit(10).to_pandas())

    print("\n======== NULL CHECK ========")
    nulls = df.select(
        *[(col(c).is_null().cast("int").alias(c+"_nulls")) for c in df.columns]
    )
    print(nulls.collect())

    print("\n======== BASIC STATS ========")
    numeric_cols = ["age", "monthly_usage", "complaints", "payments_missed", "tenure"]
    print(df.select(*numeric_cols).describe().to_pandas())

    print("\n======== CHURN DISTRIBUTION ========")
    print(df.group_by("churn_label").count().to_pandas())

if __name__ == "__main__":
    main()