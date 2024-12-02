# Databricks notebook source
# MAGIC %pip install --quiet databricks-sdk==0.9.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install --quiet git+https://github.com/lrtbrabo/DBXProfiler.git@project-reorg

# COMMAND ----------

from dbxmetrics.spark_profiler.profiler import DBXMetrics

# COMMAND ----------

def run_my_workload():
    stagemetrics = DBXMetrics("lucas_test")

    stagemetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    stagemetrics.end()

    (
        stagemetrics
        .write
        .options({"catalog": "lucas_brabo"})
        .persist("unity_catalog") 
    )
    return stagemetrics._stage_metrics(), stagemetrics._aggregate_metrics()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC create catalog if not exists __runtime_statistics;
# MAGIC create schema if not exists __runtime_statistics.metrics;

# COMMAND ----------

_stage_metrics, _aggregate_metrics = run_my_workload()

# COMMAND ----------

_stage_metrics.display()

# COMMAND ----------

_aggregate_metrics.display()
