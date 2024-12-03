# DBXMetrics

### DBXMetrics is an extension of the project [SparkMeasure](https://github.com/LucaCanali/sparkMeasure/tree/master) by Luca Canali

This project aims to extend the functionalities of the SparkMeasure project to Databricks workloads using Unity Catalog,
allowing users to monitor Databricks workloads performance without losing historical view integrated with Databricks ecosystem.

### Key Features

- **Interactive Troubleshooting:** Ideal for real-time and historical analysis of Databricks workloads in notebooks.
- **Development & CI/CD Integration:** Facilitates testing, measuring, and comparing execution metrics
  of Databricks jobs under various configurations or code changes.

### Contents

***

### Getting started with DBXMetrics

Use [this notebook](install/install_dbxmetrics.py) to install DBXMetrics in your environment. It will create the
following Unity Catalog structure (if not exists):

```
-- Catalog: __runtime_statistics
    -- Schema: metrics
```

Along with this Unity Catalog creation, it will analyse the cluster and runtime from the running cluster and install 
two main dependencies: SparkMeasure from the PyPi repository and SparkMeasure Jar from the Maven repository. Note that
this operation takes into account the environment that you're running on and will install the correct version of these
dependencies. If you're using serverless, **this will not work**.

The same notebook provides a quick test so you can familiarize yourself with the simple syntax from DBXMetrics.

***

### Notes on DBXMetrics

Spark is instrumented with several metrics, collected at task execution, they are described in [this documentation](https://spark.apache.org/docs/latest/monitoring.html#executor-task-metrics).

Some of the key metrics when looking at the report are:
- **elapsedTime:** The time taken by the stage or task to complete (in millisec)
- **executorRunTime:** The time the executors spent running the task, (in millisec). Note this time is cumulative across all tasks executed by the executor.
- **executorCpuTime:** The time the executors spent running the task, (in millisec). Note this time is cumulative across all tasks executed by the executor.
- **jvmGCTime:** The time the executors spent in garbage collection, (in millisec).
- shuffle metrics: several metrics with details on the I/O and time spend on shuffle
- I/O metrics: details on the I/O (reads and writes). Note, currently there are no time-based metrics for I/O operations.

In DBXMetrics we've added some metadata to facilitate workloads tracking:
- **__execution_timestamp**: The timestamp from when the execution happened.
- **__execution_date**: The date from when the execution happened.
- **__application_entrypoint_path**: The root path from the running notebook.
- **__application_entrypoint_file**: The notebook name.
- **__application_name**: The application name ***given by the user***.
- **__execution_id**: UUID4 identification number.
- **__runtime_version**: The cluster runtime version when the workload was executed.
- **__cluster_id**: The ID from the cluster in which the workload was executed.