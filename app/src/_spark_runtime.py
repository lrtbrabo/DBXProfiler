from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
import os
from _logger import get_logger


"""All errors gotten from this validations steps should fail the execution"""
logger = get_logger()

def get_profile():
    try:
        return os.environ.get("PROFILE"), os.environ.get("CLUSTERID")
    except:
        return None, None

def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession
        profile, cluster = get_profile()
    
        if profile and cluster:
            logger.info(f"Using profile for spark runtime: {profile} and cluster_id: {cluster}")
            return DatabricksSession.builder.profile(profile).clusterId(cluster).getOrCreate()
        else:
            try:
                 return DatabricksSession.builder.getOrCreate()
            except:
                logger.error("User not connected to a Databricks Runtime")
                raise RuntimeError("User not connected to a Databricks Runtime")
             
    except ImportError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()


def get_workspace() -> WorkspaceClient:
    try:
        from databricks.sdk import WorkspaceClient
        profile, _ = get_profile()
    
        if profile:
            logger.info(f"Using profile for workspace: {profile}")
            return WorkspaceClient(profile = profile)
        else:
            return WorkspaceClient()
    except:
        logger.error("WorkspaceClient not found")
        raise RuntimeError("User not connected to a Databricks Runtime")
