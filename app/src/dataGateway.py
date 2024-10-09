from pyspark.sql import DataFrame

"""
This part of the code is critical, 
this cannot cause a fail in the running jobs no errors should be thrown here
"""

class DataGateway:
    def __init__(self, stage_metrics: DataFrame, agg_metrics: DataFrame):
        self.stage_metrics = stage_metrics
        self.agg_metrics = agg_metrics

    def option(self, entry: str, **kwargs):
        """
        Allowed options:
            - unity_catalog:
                - catalog
            - temp
                - path
        """
        method = getattr(self, entry, None)
        if callable(method):
            return method(**kwargs)
        else:
            raise AttributeError(f"Method {entry} not found")

    def unity_catalog(self, **kwargs):
        UnityCatalog(**kwargs)

    def temp(self, **kwargs):
        TempSave(**kwargs)


class UnityCatalog:
    def __init__(self, **kargs):
        print("I'm saving this to unity!!!")


class TempSave:
    def __init__(self, **kargs):
        print("I'm saving this to temp!!!")
