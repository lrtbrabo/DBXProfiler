from pydantic import BaseModel, field_validator 
from typing import ClassVar
from _logger import get_logger
import json

logger = get_logger()

def get_valid_databricks_runtime_versions():
    with open('./config/versions.json', 'r') as f:
        data = json.load(f)
    return list(data.keys())

class EnvironmentValidator(BaseModel):
    databricks_runtime: str | None
    allowed_databricks_runtimes: ClassVar[list[str]] = get_valid_databricks_runtime_versions() 

    @field_validator("databricks_runtime")
    def validate_databricks_runtime(cls, value: str) -> str:
        if value not in cls.allowed_databricks_runtimes:
            logger.error(f"Databricks runtime should be in {cls.allowed_databricks_runtimes}. Current runtime is {value}")
            raise ValueError()
        logger.info("Databricks Runtime validated successfully")
        return value

