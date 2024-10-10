from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

"""
This part of the code is critical, 
this cannot cause a fail in the running jobs no errors should be thrown here
"""

class DataSaver(ABC):
    @abstractmethod
    def save(self, stage_metrics: DataFrame, agg_metrics: DataFrame, **kwargs):
        pass

class UnityCatalog(DataSaver):
    def save(self, stage_metrics: DataFrame, agg_metrics: DataFrame, **kwargs):
        catalog = kwargs.get("catalog")
        print(f"Saving to Unity Catalog: {catalog}")

class TempSave:
    def save(self, stage_metrics: DataFrame, agg_metrics: DataFrame, **kwargs):
        path = kwargs.get('path')
        print(f"Saving to temprary location: {path}")
        

class DataGateway:
    def __init__(self, stage_metrics: DataFrame, agg_metrics: DataFrame):
        self.stage_metrics = stage_metrics
        self.agg_metrics = agg_metrics
        self.savers = {
            'unity_catalog': UnityCatalog(),
            'temp': TempSave()
        }

    def option(self, entry: str, **kwargs):
        """
        Allowed options:
            - unity_catalog:
                - catalog
            - temp
                - path
        """
        saver = self.savers.get(entry)
        if saver:
            return saver.save(self.stage_metrics, self.agg_metrics, **kwargs)
        else:
            raise AttributeError(f"Method {entry} not found")
