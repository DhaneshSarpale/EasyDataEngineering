"""File ingestion: CSV, JSON, XML, Parquet, Avro with validation."""

from dataforge.ingestion.files.file_ingestor import FileIngestor
from dataforge.ingestion.files.readers import read_file, sniff_format

__all__ = ["read_file", "sniff_format", "FileIngestor"]
