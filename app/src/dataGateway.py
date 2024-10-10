from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class DataSaver(ABC):
    """
    Abstract interface for the savers used by the DataGateway
    """
    @abstractmethod
    def save(self, stage_metrics: DataFrame, agg_metrics: DataFrame, **kwargs):
        pass

class UnityCatalog(DataSaver):
    """
    Allowed options:
        - catalog
    """
    def save(self, stage_metrics: DataFrame, agg_metrics: DataFrame, **kwargs):
        options = self._check_for_options(**kwargs)
        catalog = options.get("catalog")
        print(f"Saving to Unity Catalog: {catalog}")

    def _check_for_options(self, **kwargs):
        allowed_options = ["catalog"]
        for option, _ in kwargs.items():
            if option not in allowed_options:
                raise ValueError(f"Option {option} does not exists")
        return kwargs 


class TempSave:
     """
    Allowed options:
        - path
    """
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
        saver = self.savers.get(entry)
        if saver:
            return saver.save(self.stage_metrics, self.agg_metrics, **kwargs)
        else:
            raise AttributeError(f"Method {entry} not found")
