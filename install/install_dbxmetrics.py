# Databricks notebook source
# MAGIC %pip install --quiet databricks-sdk==0.62.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install --quiet git+https://github.com/lrtbrabo/DBXProfiler.git@main

# COMMAND ----------

from dbxmetrics.spark_profiler.profiler import DBXMetrics
from dbxmetrics.install import EnvironmentSetup

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create catalog if not exists __runtime_statistics;
# MAGIC create schema if not exists __runtime_statistics.metrics;

# COMMAND ----------

def run_my_workload():
    stagemetrics = DBXMetrics("dbxmetrics_install_application")

    stagemetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    stagemetrics.end()

    (
        stagemetrics
        .write
        # .options({"catalog": "lucas_brabo"}) # You can pass this option to write to in another catalog, note that the schema "metrics" should be created inside of it
        .persist("unity_catalog") 
    )
    return stagemetrics._stage_metrics(), stagemetrics._aggregate_metrics()

# COMMAND ----------

_stage_metrics, _aggregate_metrics = run_my_workload()

# COMMAND ----------

_stage_metrics.display()

# COMMAND ----------

_aggregate_metrics.display()
