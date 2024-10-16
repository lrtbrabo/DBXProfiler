from sparkmeasure import StageMetrics
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, current_date, lit
from _spark_runtime import get_workspace, get_spark
from dataGateway import DataGateway
import uuid

"""
This cannot be ran from Spark Connect since Spark Context is not supported 
from Spark Connect as it depends on the JVM
"""

class DBXMetrics(StageMetrics):
    def __init__(self, application_name: str):
        self.spark = get_spark()
        self.databricks_runtime = self.spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
        self.cluster_id = self.spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

        super().__init__(self.spark)
        self.w = get_workspace()
        self.application_name = application_name
        self.execution_id = str(uuid.uuid4())

    @property
    def write(self):
        return DBXMetricsWriter(self)

    def start_metrics(self):
        self.begin()

    def stop_metrics(self):
        self.end()

    def _add_metadata(self, dataframe: DataFrame, is_legacy: bool = False) -> DataFrame:
        """
        Need to implement is this method a way to verify runtime versions to 
        check if it needs to run legacy or not
        """
        notebook_path = self.w.dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        notebook_name = notebook_path.split('/')[-1]
        if is_legacy:
            dataframe = dataframe.withColumn("__execution_timestamp", current_timestamp())
            dataframe = dataframe.withColumn("__execution_date", current_date())
            dataframe = dataframe.withColumn("__application_entrypoint_path", lit(notebook_path))
            dataframe = dataframe.withColumn("__application_entrypoint_file", lit(notebook_name)) 
            dataframe = dataframe.withColumn("__application_name", lit(self.application_name))
            dataframe = dataframe.withColumn("__execution_id", lit(self.execution_id))
            dataframe = dataframe.withColumn("__runtime_version", lit(self.databricks_runtime))
            dataframe = dataframe.withColumn("__cluster_id", lit(self.cluster_id))
            return dataframe
        else:
            dataframe = dataframe.withColumns(
                {
                    "__execution_timestamp": current_timestamp(),
                    "__execution_date": current_date(),
                    "__application_entrypoint_path": lit(notebook_path),
                    "__application_entrypoint_file": lit(notebook_name),
                    "__application_name": lit(self.application_name),
                    "__execution_id": lit(self.execution_id),
                    "__runtime_version": lit(self.databricks_runtime),
                    "__cluster_id": lit(self.cluster_id)            
                }
            )
            return dataframe
            
    def _stage_metrics(self):
        df_stage_metrics = self.create_stagemetrics_DF("PerfStageMetrics")
        df_stage_metrics = self._add_metadata(df_stage_metrics)
        return df_stage_metrics

    def _aggregate_metrics(self):
        df_aggregated_metrics = self.aggregate_stagemetrics_DF("PerfStageMetrics")
        df_aggregated_metrics = self._add_metadata(df_aggregated_metrics)
        return df_aggregated_metrics

    def persist(self, entrypoint: str, options: dict[str, str]) -> DataGateway:
        _stage_metrics = self._stage_metrics()
        _aggregate_metrics = self._aggregate_metrics()
         
        gtw = (
            DataGateway(
                stage_metrics=_stage_metrics,
                agg_metrics=_aggregate_metrics
            )
                .option(entrypoint, **options)
        )
        return gtw


class DBXMetricsWriter:
    def __init__(self, dbx_metrics: DBXMetrics):
        self._dbx_metrics = dbx_metrics
        self._options = {}

    def options(self, options: dict[str, str]):
        self._options.update(options)
        return self

    def persist(self, entrypoint: str):
        return self._dbx_metrics.persist(entrypoint, self._options)
