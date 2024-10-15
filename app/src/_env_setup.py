from _spark_runtime import get_spark, get_workspace
from _logger import get_logger
from _env_validation import EnvironmentValidator
import json
from databricks.sdk.service.compute import MavenLibrary, Library, PythonPyPiLibrary

logger = get_logger()

"""
Keeping this as two segreggated functions to avoid
future refactoring if schema needs to change
"""
def get_maven_coordinate(databricks_runtime: str) -> str:
    with open('./config/versions.json', 'r') as f:
        data = json.load(f)
    return data[databricks_runtime]["maven_coordinate"]

def get_pypi_package(databricks_runtime: str) -> str:
    with open('./config/versions.json', 'r') as f:
        data = json.load(f)
    return data[databricks_runtime]["pypi_package"]


class EnvironmentSetup:
    def __init__(self):
        self.spark = get_spark() 
        self.databricks_runtime, self.cluster_id = self._get_runtime_variables()
        self.maven_coordinate = get_maven_coordinate(self.databricks_runtime) if self.databricks_runtime else ""
        self.pypi_package = get_pypi_package(self.databricks_runtime) if self.databricks_runtime else ""
        self._validate_env = EnvironmentValidator(
                databricks_runtime = self.databricks_runtime,
            ) 
        self._install_dependencies()
        self._create_unity_dependencies()

    
    def _get_runtime_variables(self):
        databricks_runtime = self.spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
        cluster_id = self.spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        
        if databricks_runtime is None or cluster_id is None:
            databricks_runtime = ""
            cluster_id = ""

        return databricks_runtime, cluster_id

    def _install_dependencies(self):
        maven_lib = Library(
            maven = MavenLibrary(coordinates = self.maven_coordinate)
        )
        pypi_lib = Library(
            pypi = PythonPyPiLibrary(package = self.pypi_package)
        )

        w = get_workspace()
        try:
            w.libraries.install(
                cluster_id = self.cluster_id,
                libraries = [maven_lib, pypi_lib]
            )
        except Exception as e:
            logger.error(f"""Could not install the following dependencies: {self.maven_coordinate} and {self.pypi_package} 
            This is due to the following exception: {e}""")

    def _create_unity_dependencies(self):
        catalog_name = "__runtime_statistics"
        schema_name = "metrics"
        try:
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")  
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")  
        except Exception as e:
            logger.error("""Not able to create unity catalog structure, 
            please review you access. You should be able to create catalogs.""")
            raise e 
            
run = EnvironmentSetup()















