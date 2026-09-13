"""DataForge - end-to-end AWS data engineering reference platform.

The package is organised by data-engineering concern:

- ``common``          shared config, logging, metadata, idempotency, errors
- ``data_generation`` synthetic banking data generator (Faker)
- ``ingestion``       batch / incremental / file / api / sftp / cdc patterns
- ``transformations`` pandas + PySpark transforms, SCD, aggregations
- ``data_quality``    rule engine, custom checks, quarantine, reporting
- ``streaming``       Kinesis producer/consumer + local demo + fraud rules
- ``pipelines``       runnable end-to-end orchestrations (local mode)

Everything runs in LOCAL MODE without an AWS account. AWS-specific
resources are provided as deployable Terraform / clearly-labelled
reference code under the repository's top-level directories.
"""

__version__ = "0.1.0"
