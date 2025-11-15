-- 1. Count total customers
SELECT COUNT(*) AS total_customers 
FROM CUSTOMER_CHURN;

-- 2. Churn rate
SELECT 
    SUM(churn_label) / COUNT(*) AS churn_rate
FROM CUSTOMER_CHURN;

-- 3. Breakdown by plan
SELECT 
    plan,
    COUNT(*) AS total_users,
    SUM(churn_label) AS churned_users,
    SUM(churn_label) / COUNT(*) AS churn_percentage
FROM CUSTOMER_CHURN
GROUP BY plan
ORDER BY churn_percentage DESC;

-- 4. High-risk users (according to model UDF)
SELECT *,
       predict_churn(age, city, plan, monthly_usage, complaints, payments_missed, tenure)
       AS predicted_churn
FROM CUSTOMER_CHURN
ORDER BY predicted_churn DESC;