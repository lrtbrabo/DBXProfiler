# Databricks notebook source

from DBXMetrics import DBXMetrics
from _spark_runtime import get_spark

def run_my_workload():
    spark = get_spark()
    stagemetrics = DBXMetrics("lucas_test", "test_catalog")

    stagemetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    stagemetrics.end()

    stagemetrics.persist() 

run_my_workload()
