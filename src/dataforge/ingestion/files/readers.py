"""Format-aware file readers.

Supports CSV, JSON (incl. nested), XML, Parquet and Avro. Each reader
returns a flat ``pandas.DataFrame``. Avro and XML are optional-dependency
formats; a clear error is raised if the extra isn't installed.

Nested JSON is flattened (dotted column names) so it lands as tabular
bronze data - the classic "flatten during transformation" step.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from dataforge.common.errors import NonRetryableError, SchemaValidationError
from dataforge.common.logging_utils import get_logger

_log = get_logger("dataforge.ingestion.files")

_EXT_TO_FORMAT = {
    ".csv": "csv",
    ".json": "json",
    ".xml": "xml",
    ".parquet": "parquet",
    ".pq": "parquet",
    ".avro": "avro",
}


def sniff_format(path: str | Path) -> str:
    """Infer the file format from its extension."""
    ext = Path(path).suffix.lower()
    fmt = _EXT_TO_FORMAT.get(ext)
    if fmt is None:
        raise NonRetryableError(f"Unsupported file extension: {ext}")
    return fmt


def flatten_json(obj: Any, prefix: str = "", sep: str = ".") -> dict[str, Any]:
    """Recursively flatten a nested dict into dotted keys.

    Lists are JSON-encoded rather than exploded, keeping the flattener
    simple and lossless; explosion is a transformation concern.
    """
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            new_key = f"{prefix}{sep}{key}" if prefix else key
            out.update(flatten_json(value, new_key, sep))
    elif isinstance(obj, list):
        out[prefix] = json.dumps(obj)
    else:
        out[prefix] = obj
    return out


def _read_json(path: Path) -> pd.DataFrame:
    doc = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(doc, dict):
        list_fields = [k for k, v in doc.items() if isinstance(v, list)]
        records = doc[list_fields[0]] if list_fields else [doc]
    elif isinstance(doc, list):
        records = doc
    else:
        raise SchemaValidationError("JSON root must be an object or array")
    return pd.DataFrame([flatten_json(r) for r in records])


def _read_xml(path: Path) -> pd.DataFrame:
    try:
        from lxml import etree
    except ImportError as exc:  # pragma: no cover - optional dep
        raise NonRetryableError("XML support requires the 'files' extra (lxml)") from exc
    tree = etree.parse(str(path))
    root = tree.getroot()
    rows: list[dict[str, Any]] = []
    for child in root:
        row: dict[str, Any] = dict(child.attrib)
        for elem in child:
            row[elem.tag] = elem.text
        rows.append(row)
    return pd.DataFrame(rows)


def _read_avro(path: Path) -> pd.DataFrame:
    try:
        import fastavro
    except ImportError as exc:  # pragma: no cover - optional dep
        raise NonRetryableError("Avro support requires the 'files' extra (fastavro)") from exc
    with path.open("rb") as fh:
        return pd.DataFrame(list(fastavro.reader(fh)))


def read_file(path: str | Path, fmt: str | None = None) -> pd.DataFrame:
    """Read any supported file into a DataFrame.

    Raises:
        NonRetryableError: unsupported format / empty file.
        SchemaValidationError: content cannot be parsed into rows.
    """
    path = Path(path)
    if not path.exists():
        raise NonRetryableError(f"File not found: {path}")
    if path.stat().st_size == 0:
        raise NonRetryableError(f"Empty file: {path}")

    fmt = fmt or sniff_format(path)
    try:
        if fmt == "csv":
            return pd.read_csv(path)
        if fmt == "json":
            return _read_json(path)
        if fmt == "xml":
            return _read_xml(path)
        if fmt == "parquet":
            return pd.read_parquet(path, engine="pyarrow")
        if fmt == "avro":
            return _read_avro(path)
    except (ValueError, json.JSONDecodeError) as exc:
        raise SchemaValidationError(f"Malformed {fmt} file {path}: {exc}") from exc
    raise NonRetryableError(f"Unsupported format: {fmt}")
