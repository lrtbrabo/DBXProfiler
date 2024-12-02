from pyspark.sql import DataFrame


class DataSaver:
    """
    Abstract interface for the savers used by the DataGateway
    """

    def save(self, stage_metrics: DataFrame, agg_metrics: DataFrame, **kwargs):
        pass

    @staticmethod
    def _check_for_options(expected_options: list[str], allowed_options: list[str], **kwargs):
        """
        In the future, this implementation should be changed to accept multiple "expected_options", 
        for now, we will put only the initial one. We should accept the expected_options just as as
        1 position array in order to not impact future calls and guarantee backwards-compatibility.
        """
        if expected_options[0] not in kwargs.keys():
            raise ValueError(f"Missing '{expected_options}' option")
        for option, _ in kwargs.items():
            if option not in allowed_options:
                raise ValueError(f"Option {option} does not exists")
        return kwargs

class UnityCatalog(DataSaver):
    """
    Allowed options:
        - catalog
    """

    @staticmethod
    def _get_catalog_and_schema():
        catalog = "__runtime_statistics"
        schema = "metrics"
        return catalog, schema

    def save(self, stage_metrics: DataFrame, agg_metrics: DataFrame, **kwargs):
        expected_options = ["catalog"]
        allowed_options = ["catalog"]
        options = self._check_for_options(
            expected_options=expected_options,
            allowed_options=allowed_options,
            **kwargs
        )
        # catalog = options.get("catalog")
        catalog_name, schema_name = self._get_catalog_and_schema()
        stage_metrics.write.mode("append").saveAsTable(f"{catalog_name}.{schema_name}.task_metrics")
        agg_metrics.write.mode("append").saveAsTable(f"{catalog_name}.{schema_name}.task_agg_metrics")
        print(f"Saving to Unity Catalog")



class TempSave(DataSaver):
    """
    Allowed options:
        - path
    """
    def save(self, stage_metrics: DataFrame, agg_metrics: DataFrame, **kwargs):
        expected_options = ["path"]
        allowed_options = ["path"]
        options = self._check_for_options(
            expected_options=expected_options,
            allowed_options=allowed_options,
            **kwargs
        )
        path = options.get("path")
        print(f"Saving to temp location: {path}")

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
