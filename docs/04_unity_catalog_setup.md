# Unity Catalog Setup

In this step you create:
- one catalog
- multiple schemas
- governed volumes

Suggested structure:
- catalog: `adesso_dev`
- schemas:
  - `raw`
  - `refined`
  - `analytics`

Volumes:
- `landing`
- `checkpoints`

Use `sql/01_create_uc_objects.sql`.
