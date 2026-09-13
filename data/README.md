# data/

Working directory for local artefacts. **Generated content is gitignored**
(see `.gitignore`) — regenerate it with `make generate-data`.

| Path | Contents | Committed? |
| --- | --- | --- |
| `data/generated/` | synthetic source DB + csv/json/xml/parquet/sftp_inbox | no (generated) |
| `data/lake/` | local medallion lake (raw/bronze/silver/gold/quarantine) | no (generated) |
| `data/warehouse/` | SQLite warehouse + metadata + DQ report | no (generated) |
| `data/sample/` | a few tiny committed sample rows for quick inspection | yes |

The `sample/`, `csv/`, `json/`, `xml/`, `sql/` sub-folders referenced in
docs are produced under `data/generated/` by the generator; only small
curated samples live in `data/sample/`.
