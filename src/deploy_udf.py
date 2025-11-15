from snowflake.snowpark import Session
from snowflake.ml.model import load_model

session = Session.builder.configs(connection_params).create()

@session.udf(
    name="predict_churn",
    replace=True,
    is_permanent=True,
    stage_location="@model_stage/udfs"
)
def predict_churn_udf(age:int, city:str, plan:str, monthly_usage:float,
                      complaints:int, payments_missed:int, tenure:int):
    model = load_model(session, "@model_stage/churn_model")
    sample = [{"age": age, "city": city, "plan": plan,
               "monthly_usage": monthly_usage,
               "complaints": complaints,
               "payments_missed": payments_missed,
               "tenure": tenure}]
    return int(model.predict(sample)[0])