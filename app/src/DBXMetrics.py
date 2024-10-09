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
        super().__init__(get_spark())
        self.w = get_workspace()
        self.application_name = application_name
        self.execution_id = str(uuid.uuid4())

    def start_metrics(self):
        self.begin()

    def stop_metrics(self):
        self.end()

    def _add_metadata(self, dataframe: DataFrame) -> DataFrame:
        """
        Using the method withColumn instead of with Columns
        for backwards compatibility with previous spark versions
        """
        notebook_path = self.w.dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        notebook_name = notebook_path.split('/')[-1] 
        dataframe = dataframe.withColumn("__execution_timestamp", current_timestamp())
        dataframe = dataframe.withColumn("__execution_date", current_date())
        dataframe = dataframe.withColumn("__application_entrypoint_path", lit(notebook_path))
        dataframe = dataframe.withColumn("__application_entrypoint_file", lit(notebook_name)) 
        dataframe = dataframe.withColumn("__application_name", lit(self.application_name))
        dataframe = dataframe.withColumn("__execution_id", lit(self.execution_id))
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
        """
        Allowed options:
            - unity_catalog:
                - catalog
            - temp
                - path
        """
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


