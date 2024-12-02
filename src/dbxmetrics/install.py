from databricks.sdk.service.compute import MavenLibrary, Library, PythonPyPiLibrary
from dataclasses import dataclass
from typing import ClassVar
from dbxmetrics.config import WorkspaceConfig, get_spark, get_workspace
from dbxmetrics._logger import get_logger
import json

logger = get_logger()
workspace_config = WorkspaceConfig()

@dataclass
class EnvironmentValidator:
    databricks_runtime: str | None = workspace_config.get_databricks_runtime()
    allowed_databricks_runtimes: ClassVar[list[str]] = workspace_config.allowed_databricks_runtimes

    def __post_init__(self):
        if self.databricks_runtime not in self.allowed_databricks_runtimes:
            logger.error(
                f"Databricks runtime should be in {self.allowed_databricks_runtimes}. Current runtime is {self.databricks_runtime}")
            raise ValueError()
        logger.info("Databricks Runtime validated successfully")
        return self.databricks_runtime


class EnvironmentSetup:
    def __init__(self):
        self.spark = get_spark()
        self.databricks_runtime = workspace_config.get_databricks_runtime()
        self.cluster_id = workspace_config.get_cluster()
        self.maven_coordinate = workspace_config.get_maven_coordinate() if self.databricks_runtime else ""
        self.pypi_package = workspace_config.get_pypi_package() if self.databricks_runtime else ""
        self._validate_env = EnvironmentValidator(
            databricks_runtime=self.databricks_runtime,
        )
        self._install_dependencies()
        # self._create_unity_dependencies()

    def _install_dependencies(self):
        maven_lib = Library(
            maven=MavenLibrary(coordinates=self.maven_coordinate)
        )
        pypi_lib = Library(
            pypi=PythonPyPiLibrary(package=self.pypi_package)
        )

        w = get_workspace()
        try:
            w.libraries.install(
                cluster_id=self.cluster_id,
                libraries=[maven_lib, pypi_lib]
            )
        except Exception as e:
            logger.error(
                f"""Could not install the following dependencies: {self.maven_coordinate} and {self.pypi_package} 
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















