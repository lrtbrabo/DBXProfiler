from _spark_runtime import get_spark, get_workspace
from _logger import get_logger
from _env_validation import EnvironmentValidator
import json
from databricks.sdk.service.compute import MavenLibrary, Library, PythonPyPiLibrary

logger = get_logger()

#Keeping this as two segreggated functions to avoid
#future refactoring if schema needs to change
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
        self.databricks_runtime, self.cluster_id, self.maven_coordinate, self.pypi_package = self.define_runtime_variables()
        self._validate_env()
        self._install_dependencies()

    def define_runtime_variables(self):
        databricks_runtime = self.spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
        cluster_id = self.spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        
        if databricks_runtime is None or cluster_id is None:
            databricks_runtime = ""
            cluster_id = ""

        maven_coordinate = get_maven_coordinate(databricks_runtime) if databricks_runtime else ""
        pypi_package = get_pypi_package(databricks_runtime) if databricks_runtime else ""
        return databricks_runtime, cluster_id, maven_coordinate, pypi_package


    def _validate_env(self):
        try:    
            EnvironmentValidator(
                databricks_runtime = self.databricks_runtime,
            )
        except ValueError as e:
            logger.error(e)


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


run = EnvironmentSetup()
