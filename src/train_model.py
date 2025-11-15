from snowflake.snowpark import Session
from snowflake.ml.modeling.preprocessing import OneHotEncoder, StandardScaler
from snowflake.ml.modeling import LogisticRegression
from snowflake.ml.model import save_model

connection_params = {
    "account": "<your_account>",
    "user": "<your_user>",
    "password": "<your_password>",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "CHURN_DB",
    "schema": "PUBLIC"
}

def main():
    session = Session.builder.configs(connection_params).create()

    df = session.table("CUSTOMER_CHURN")

    feature_cols = [
        "age", "city", "plan", "monthly_usage",
        "complaints", "payments_missed", "tenure"
    ]
    target = "churn_label"

    # Encode categorical variables
    encoder = OneHotEncoder(input_cols=["city", "plan"])
    df_encoded = encoder.fit(df).transform(df)

    # Scale numeric features
    scaler = StandardScaler(
        input_cols=["age", "monthly_usage", "complaints", "payments_missed", "tenure"]
    )
    df_scaled = scaler.fit(df_encoded).transform(df_encoded)

    # Train/test split
    train_df, test_df = df_scaled.randomSplit([0.8, 0.2])

    # Model
    model = LogisticRegression(label_col=target)
    fitted = model.fit(train_df)

    # Save model
    save_model(
        model=fitted,
        session=session,
        stage_location="@model_stage/churn_model"
    )

    print("🎉 Model successfully trained and saved in @model_stage/churn_model")

if __name__ == "__main__":
    main()