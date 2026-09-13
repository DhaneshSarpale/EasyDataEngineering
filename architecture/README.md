# Architecture diagrams

The architecture is documented as **Mermaid** diagrams (rendered inline by
GitHub) in this folder and in [`../docs/architecture.md`](../docs/architecture.md).

- `architecture.mmd` — overall platform
- `batch_architecture.mmd` — batch/medallion flow
- `streaming_architecture.mmd` — Kinesis + fraud path
- `cdc_architecture.mmd` — DMS full-load + CDC path

## Rendering to PNG (optional)

Mermaid `.mmd` sources render on GitHub automatically inside Markdown. To
export standalone PNGs (e.g. `architecture.png`) use the Mermaid CLI:

```bash
npm install -g @mermaid-js/mermaid-cli
mmdc -i architecture/architecture.mmd -o architecture/architecture.png
```

A `.drawio` file can be created by importing the Mermaid source into
[draw.io](https://draw.io) if an editable diagram is preferred.
