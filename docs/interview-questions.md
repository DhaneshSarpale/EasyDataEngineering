# Data Engineering interview questions (100+)

Concise Q&A across the topics DataForge covers. Answers are kept short and
practical; where relevant they point at the matching code in this repo.

## AWS S3 (1-8)

1. **What is S3 and why is it the backbone of a data lake?** Object storage
   with 11-nines durability, effectively unlimited scale, cheap, decoupled
   from compute; integrates with Glue/Athena/EMR/Redshift Spectrum.
2. **S3 storage classes?** Standard, Standard-IA, One Zone-IA,
   Intelligent-Tiering, Glacier (Instant/Flexible/Deep Archive). DataForge
   lifecycles raw → IA (30d) → Glacier (90d).
3. **What are S3 "prefixes" and how do they relate to partitions?**
   Key prefixes (e.g. `year=2025/month=01/`) act as Hive partitions; query
   engines prune on them.
4. **How do you secure an S3 bucket?** Block public access, SSE-KMS, bucket
   policies, VPC endpoints, versioning, access logging.
5. **SSE-S3 vs SSE-KMS?** Both encrypt at rest; KMS adds an auditable,
   rotatable customer-managed key + fine-grained access (at extra request
   cost — mitigate with bucket keys).
6. **How does S3 achieve consistency?** Strong read-after-write consistency
   for all operations since 2020.
7. **What is multipart upload?** Splits large objects into parts uploaded
   in parallel; resumable and faster.
8. **How do you avoid the "small files problem"?** Compact into larger
   Parquet files (coalesce), use partitioning sensibly, avoid millions of
   tiny objects that slow scans and inflate request cost.

## AWS Glue (9-16)

9. **What is AWS Glue?** Serverless Spark ETL + a Data Catalog + crawlers.
10. **DynamicFrame vs DataFrame?** DynamicFrame is Glue's schema-flexible
    structure (choice types, catalog/bookmark integration) for messy input;
    convert to DataFrame for Catalyst-optimised transforms.
11. **What is the Glue Data Catalog?** A central metastore of table schemas
    over S3, shared by Athena/Redshift Spectrum/EMR.
12. **What are Glue crawlers?** Jobs that infer schema + partitions from S3
    and register/update catalog tables.
13. **What are Glue job bookmarks?** Built-in state that tracks processed
    data so a job only reads new data on reruns (incremental).
14. **How is a Glue job billed?** Per DPU-hour (~$0.44), 1-minute minimum.
15. **G.1X vs G.2X workers?** Memory/CPU per worker; G.2X for
    memory-heavy/shuffle-heavy jobs, fewer workers.
16. **How do you make Glue incremental?** Bookmarks, or a pushdown WHERE on
    a watermark column (see `jdbc_reference.py`).

## AWS Athena (17-22)

17. **What is Athena?** Serverless Trino/Presto SQL over S3; pay per TB
    scanned.
18. **How do you reduce Athena cost?** Columnar Parquet, partitioning +
    pruning, `SELECT` only needed columns, compression, partition
    projection.
19. **External vs managed table?** Athena tables are external (data stays
    in S3); dropping a table doesn't delete data.
20. **What is CTAS?** `CREATE TABLE AS SELECT` — materialises query results
    back to S3 as Parquet and registers a table.
21. **Partition projection?** Compute partitions from a pattern instead of
    crawling/MSCK — avoids crawler cost and speeds planning.
22. **Can Athena query nested JSON?** Yes — struct/array types with dot/
    array access.

## Amazon Redshift (23-31)

23. **What is Redshift?** Columnar MPP data warehouse.
24. **DISTKEY / distribution styles?** KEY (co-locate join keys on the same
    slice), ALL (replicate small dims), EVEN, AUTO. Choose KEY on the most
    joined column.
25. **SORTKEY?** Orders rows on disk → zone-map pruning for range filters;
    compound vs interleaved.
26. **Column compression (ENCODE)?** Per-column codec (AZ64/ZSTD/BYTEDICT)
    to cut I/O; `ANALYZE COMPRESSION` suggests encodings.
27. **COPY command?** Parallel bulk load from S3 across slices; use it, not
    row INSERTs, for big loads.
28. **How to UPSERT in Redshift?** Staging table + `DELETE ... USING` +
    `INSERT` in a transaction (or `MERGE`). See `redshift_setup.sql`.
29. **VACUUM vs ANALYZE?** VACUUM reclaims space + re-sorts after churn;
    ANALYZE refreshes planner statistics.
30. **Redshift Spectrum?** Query S3 external tables from Redshift without
    loading.
31. **RA3 vs DC2 nodes?** RA3 separates compute from managed storage
    (scale independently); DC2 has local SSD.

## Kinesis & streaming (32-40)

32. **Kinesis Data Streams vs Firehose?** Streams = low-latency, custom
    consumers, shards you manage; Firehose = managed delivery to
    S3/Redshift/OpenSearch with buffering, no consumer code.
33. **What is a shard?** A unit of capacity (1 MB/s or 1,000 rec/s in;
    2 MB/s out). Throughput scales with shard count.
34. **Partition key role?** Determines the shard → same key = same shard =
    ordered. DataForge uses `customer_id` to keep a customer's events
    ordered.
35. **How do you scale Kinesis?** Add shards (resharding) or use on-demand
    mode (auto-scales, pay per GB).
36. **What is the KCL?** Kinesis Client Library — handles shard leases,
    checkpointing (to DynamoDB), and rebalancing for consumers.
37. **Exactly-once vs at-least-once?** Kinesis is at-least-once; achieve
    effectively-once with idempotent consumers (dedupe by event id — see
    `StreamConsumer`).
38. **Late-arriving events?** Handle with event-time windows + watermarks;
    don't let a late event corrupt state (DataForge ignores them for the
    travel baseline).
39. **What is iterator age?** How far behind a consumer is; a key lag
    metric to alarm on.
40. **Firehose buffering tradeoff?** Bigger buffer = fewer/larger files
    (cheaper Athena) but higher latency.

## DMS & CDC (41-47)

41. **What is CDC?** Capturing row-level INSERT/UPDATE/DELETE from a source
    to propagate changes downstream.
42. **Full load vs CDC vs full-load+CDC?** Snapshot; ongoing changes; both
    (snapshot then apply the stream).
43. **Log-based vs query-based CDC?** Log-based reads the WAL/binlog (low
    impact, captures deletes); query-based polls a timestamp (misses hard
    deletes).
44. **What is AWS DMS?** Managed migration/replication service supporting
    full-load + CDC to many targets including S3.
45. **How do you reconstruct current state from CDC?** Apply events per PK
    in timestamp order: upsert after_image, remove on DELETE. See
    `reconstruct_latest_state`.
46. **What are before/after images?** Row state before and after the change
    — needed for updates/deletes and auditing.
47. **CDC challenges?** Ordering, schema evolution, deletes, exactly-once,
    handling the initial snapshot boundary.

## Lambda (48-52)

48. **When is Lambda a good fit for data?** Short, event-driven, bursty
    work: S3-triggered validation, kicking off Glue/SFN, light API pulls.
49. **When is Lambda a bad fit?** Long/heavy jobs (15-min, 10 GB caps),
    sustained high-throughput streaming, big JVM/Spark runtimes.
50. **How to handle Lambda failures?** DLQ (SQS/SNS), retries, idempotent
    handlers.
51. **Cold starts?** Init latency on first/scaled invokes; mitigate with
    provisioned concurrency / smaller packages.
52. **How does Lambda log?** stdout → CloudWatch Logs automatically
    (DataForge writes JSON).

## Spark / PySpark (53-64)

53. **RDD vs DataFrame?** DataFrame is higher-level, Catalyst-optimised,
    columnar; prefer it.
54. **What is a shuffle?** Redistributing data across the cluster (wide
    transforms: join/groupBy/distinct) — expensive; minimise it.
55. **Narrow vs wide transformation?** Narrow (map/filter) no shuffle; wide
    (join/groupBy) shuffle.
56. **Driver vs executor?** Driver builds the DAG/coordinates; executors run
    tasks and hold cache/shuffle data.
57. **Stage vs task?** A stage = tasks with no shuffle between them; a task
    processes one partition.
58. **What is a broadcast join?** Ship a small table to all executors to
    avoid shuffling the large one.
59. **repartition vs coalesce?** repartition = full shuffle to more/even
    partitions; coalesce = merge partitions with no shuffle (reduce small
    files).
60. **Predicate pushdown / column pruning?** Push filters/column selection
    to the scan so less data is read (great with Parquet).
61. **When to cache?** When a DataFrame is reused across multiple actions.
62. **What causes data skew and how to fix it?** Uneven key distribution;
    fix with salting, AQE skew join, or broadcast.
63. **What is AQE?** Adaptive Query Execution — runtime re-optimisation
    (coalesce partitions, skew handling, join strategy switch).
64. **spark.sql.shuffle.partitions?** Number of post-shuffle partitions;
    tune to data size (default 200 is often too many for small data).

## SQL (65-74)

65. **WHERE vs HAVING?** WHERE filters rows pre-aggregation; HAVING filters
    groups post-aggregation.
66. **INNER/LEFT/RIGHT/FULL join?** Matching only / all left / all right /
    all both.
67. **SEMI vs ANTI join?** SEMI = left rows with a match (no right cols);
    ANTI = left rows without a match.
68. **What is a window function?** Aggregates over a row window without
    collapsing rows (ROW_NUMBER, RANK, LAG/LEAD, SUM OVER).
69. **ROW_NUMBER vs RANK vs DENSE_RANK?** Unique sequential; ties share rank
    with gaps; ties share rank without gaps.
70. **CTE vs subquery?** CTE (`WITH`) is a named, readable, reusable inline
    view; recursive CTEs handle hierarchies.
71. **UNION vs UNION ALL?** UNION dedupes (sort cost); UNION ALL keeps
    duplicates (faster).
72. **INTERSECT / EXCEPT (MINUS)?** Common rows / rows in first not second.
73. **COALESCE vs NULLIF?** First non-null / null when two values are equal
    (divide-by-zero guard).
74. **How to dedupe in SQL?** `ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts
    DESC) = 1`.

## Python & ETL/ELT (75-82)

75. **ETL vs ELT?** Transform before load vs load raw then transform in the
    warehouse/lake (ELT suits cheap columnar storage + powerful SQL).
76. **What is idempotency and why care?** Re-running produces the same
    result; prevents double counting on retries. See `IdempotencyStore`.
77. **Retryable vs non-retryable errors?** Transient (retry with backoff)
    vs deterministic (fail fast → DLQ/quarantine). See `errors.py`.
78. **Exponential backoff + jitter?** Increasing delays + randomness to
    avoid thundering-herd retries.
79. **What is a watermark?** The high-water mark of successfully processed
    data; advanced only after success for safe reruns.
80. **How do you handle bad records?** Detect → reject → quarantine with a
    reason → log → keep processing good records.
81. **Backfill?** Reprocessing historical partitions; needs idempotent,
    partition-scoped jobs.
82. **Schema evolution?** Handle added/renamed columns; formats like
    Avro/Parquet + a catalog help; validate on ingest.

## Data lake / warehouse / modelling (83-90)

83. **Data lake vs warehouse vs lakehouse?** Raw multi-format storage;
    curated structured analytics store; lake + warehouse features (ACID
    tables on object storage).
84. **Medallion architecture?** raw → bronze → silver → gold hops with
    clear contracts.
85. **Star vs snowflake schema?** Denormalised dims (fewer joins) vs
    normalised dims (more joins, less redundancy).
86. **Fact grain — why does it matter?** Defines what one row means; mixing
    grains breaks aggregations.
87. **Natural vs surrogate key?** Business id vs system-generated key;
    surrogates enable SCD2 versioning + insulate from source changes.
88. **SCD Type 1 vs 2 vs 3?** Overwrite / new versioned row with validity
    dates / add a "previous value" column.
89. **Conformed dimension?** A dimension shared consistently across facts
    (e.g. `dim_date`).
90. **Slowly vs rapidly changing dimension?** SCD2 is impractical for
    high-churn attributes → use a mini-dimension or a fact-side attribute.

## Data quality (91-94)

91. **Common DQ dimensions?** Completeness, uniqueness, validity,
    consistency, timeliness, accuracy.
92. **Great Expectations vs Glue Data Quality vs custom?** GE = rich
    expectations + Data Docs; Glue DQ = managed, DQDL rules in Glue; custom
    = zero-dep, full control (DataForge default).
93. **Row-level vs table-level checks?** Per-row boolean (quarantine
    failing rows) vs whole-table assertion (e.g. uniqueness).
94. **Where do bad records go?** A quarantine zone with a reason, so valid
    data still flows and bad data is investigable.

## Airflow / dbt (95-100)

95. **Airflow DAG/operator/task?** DAG = pipeline+schedule; operator = unit
    of work type; task = an operator instance.
96. **What is XCom?** Small cross-task message passing (e.g. row counts).
97. **Airflow retries/backoff?** Per-task `retries` + `retry_delay` +
    exponential backoff.
98. **dbt model materializations?** view / table / incremental / ephemeral;
    incremental only processes new rows.
99. **dbt tests?** Generic (not_null/unique/accepted_values/relationships)
    + singular (bespoke SQL returning failing rows).
100. **dbt snapshots?** Managed SCD2 — dbt tracks changes and maintains
    `dbt_valid_from`/`dbt_valid_to`.

## Terraform / CI-CD / Security (101-108)

101. **What is IaC and why?** Infrastructure defined as versioned code —
    reproducible, reviewable, auditable.
102. **Terraform state — what/why lock?** Records real resources; locked
    (DynamoDB) to prevent concurrent corruption; store remotely (S3).
103. **Terraform module?** Reusable, parameterised bundle of resources
    (DataForge: s3, kms, iam, glue, ...).
104. **plan vs apply?** Preview the diff vs execute it.
105. **What is GitHub OIDC for AWS?** GitHub Actions gets a short-lived
    token exchanged for temporary AWS creds — no long-lived keys stored.
106. **Least privilege?** Grant only the actions/resources a role needs;
    scope to concrete ARNs; add permission boundaries.
107. **How do you manage secrets?** Secrets Manager / SSM Parameter Store /
    env vars — never in code or config; rotate; don't log values.
108. **Encryption at rest vs in transit?** SSE-KMS on S3/Kinesis/Redshift
    vs TLS on the wire.

## Performance / cost / scale (109-112)

109. **How do you make a Spark job cheaper/faster?** Filter early, prune
    columns, partition + prune, broadcast small dims, avoid unnecessary
    shuffles, right-size workers.
110. **How do you control Athena cost?** Parquet + partitions + column
    pruning; bill is per TB scanned.
111. **How do you keep a portfolio AWS bill low?** Prefer serverless/
    on-demand, avoid always-on Redshift/MWAA, destroy after demos, set a
    Budget alert.
112. **How would you scale this to real volumes?** Swap pandas→Glue/EMR,
    SQLite→Redshift, LocalStream→Kinesis, add Iceberg for ACID + time
    travel, and autoscale consumers.
