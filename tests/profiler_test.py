# Databricks notebook source
# MAGIC %pip install --quiet databricks-sdk==0.9.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------


from src.dbxmetrics.spark_profiler.profiler import DBXMetrics

# COMMAND ----------

def run_my_workload():
    stagemetrics = DBXMetrics("lucas_test")

    stagemetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    stagemetrics.end()

    (
        stagemetrics
        .write
        .options({"catalog": "temp_lucas"})
        .persist("unity_catalog") 
    )
    return stagemetrics._stage_metrics(), stagemetrics._aggregate_metrics()

# COMMAND ----------

_stage_metrics, _aggregate_metrics = run_my_workload()

# COMMAND ----------

_stage_metrics.show()

# COMMAND ----------

_aggregate_metrics.show()

# COMMAND ----------


