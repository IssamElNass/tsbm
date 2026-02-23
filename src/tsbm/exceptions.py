class TsbmError(Exception):
    """Base exception for all tsbm errors."""


class ConnectionError(TsbmError):
    """Failed to connect to a database."""


class IngestionError(TsbmError):
    """Error during data ingestion."""


class QueryError(TsbmError):
    """Error executing a benchmark query."""


class ConfigError(TsbmError):
    """Invalid or missing configuration."""


class SchemaError(TsbmError):
    """Dataset schema inference or validation failure."""


class DatasetError(TsbmError):
    """Error loading or generating a dataset."""
