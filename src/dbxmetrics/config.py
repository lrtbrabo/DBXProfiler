from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
import os
from dbxmetrics._logger import get_logger
from dataclasses import dataclass, field

"""All errors gotten from this validations steps should fail the execution"""
logger = get_logger()

class SparkSingleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            try:
                from databricks.connect import DatabricksSession
                profile = _get_profile()
                cluster = _get_cluster()

                if profile and cluster:
                    logger.info(f"Using profile for spark runtime: {profile} and cluster_id: {cluster}")
                    cls._instance = DatabricksSession.builder.profile(profile).clusterId(cluster).getOrCreate()
                else:
                    try:
                        cls._instance = DatabricksSession.builder.getOrCreate()
                    except:
                        logger.error("User not connected to a Databricks Runtime")
                        raise RuntimeError("User not connected to a Databricks Runtime")

            except ImportError:
                from pyspark.sql import SparkSession
                cls._instance = SparkSession.builder.getOrCreate()

        return cls._instance

class WorkspaceSingleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            try:
                from databricks.sdk import WorkspaceClient
                profile = _get_profile()

                if profile:
                    logger.info(f"Using profile for workspace: {profile}")
                    cls._instance = WorkspaceClient(profile=profile)
                else:
                    cls._instance = WorkspaceClient()
            except:
                logger.error("WorkspaceClient not found")
                raise RuntimeError("User not connected to a Databricks Runtime")

        return cls._instance

def get_spark() -> SparkSession:
    return SparkSingleton()

def get_workspace() -> WorkspaceClient:
    return WorkspaceSingleton()

def _get_allowed_databricks_runtimes():
    return {
        "13.3.x-scala2.12": {
            "maven_coordinate": "ch.cern.sparkmeasure:spark-measure_2.12:0.24",
            "pypi_package": "sparkmeasure==0.24.0"
        }
    }

def _get_profile() -> str | None:
    try:
        return os.environ.get("PROFILE")
    except:
        return None

def _get_cluster() -> str | None:
    try:
        return os.environ.get("CLUSTER")
    except:
        return None

def _get_databricks_runtime():
    spark = get_spark()
    return spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")

def _get_cluster_id():
    spark = get_spark()
    return spark.conf.get("spark.databricks.clusterUsageTags.clusterId")


@dataclass
class WorkspaceConfig:
    databricks_runtime: str | None = field(default_factory=_get_databricks_runtime)
    allowed_databricks_runtimes: dict[str] = field(default_factory = _get_allowed_databricks_runtimes)
    cluster_id: str | None = field(default_factory=_get_cluster)

    def get_databricks_runtime(self) -> str:
        if self.databricks_runtime:
            return self.databricks_runtime
        return ""

    def get_cluster(self) -> str:
        if self.cluster_id:
            return self.cluster_id
        return ""

    def get_maven_coordinate(self):
        maven_coord = self.allowed_databricks_runtimes[self.databricks_runtime]["maven_coordinate"]
        logger.info(f"Maven coordinate: {maven_coord}")
        return self.allowed_databricks_runtimes[self.databricks_runtime]["maven_coordinate"]

    def get_pypi_package(self):
        pypi_package =  self.allowed_databricks_runtimes[self.databricks_runtime]["pypi_package"]
        logger.info(f"Pypi package: {pypi_package}")
        return self.allowed_databricks_runtimes[self.databricks_runtime]["pypi_package"]