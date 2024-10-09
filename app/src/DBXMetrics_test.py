# Databricks notebook source
# MAGIC %pip install --quiet databricks-sdk==0.9.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from DBXMetrics import DBXMetrics
from _spark_runtime import get_spark

# COMMAND ----------

def run_my_workload():
    spark = get_spark()
    stagemetrics = DBXMetrics("lucas_test")

    stagemetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    stagemetrics.end()

    stagemetrics.persist("unity_catalog", {"catalog": "temp_lucas"}) 
    return stagemetrics._stage_metrics(), stagemetrics._aggregate_metrics()

# COMMAND ----------

_stage_metrics, _aggregate_metrics = run_my_workload()

# COMMAND ----------

_stage_metrics.display()

# COMMAND ----------

_aggregate_metrics.display()

# COMMAND ----------


