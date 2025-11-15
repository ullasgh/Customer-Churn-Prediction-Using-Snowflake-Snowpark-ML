CREATE OR REPLACE TABLE CUSTOMER_CHURN (
  customer_id INT,
  age INT,
  city STRING,
  plan STRING,
  monthly_usage FLOAT,
  complaints INT,
  payments_missed INT,
  tenure INT,
  churn_label INT
);