# Client-Specific Logic

All client-specific logic lives under `clients/{Client}/`. This enables **CI/CD**: building for client X = `common/` + `clients/X/` only.

## Structure

```
clients/
  {Client}/
    __init__.py
    parser.py        # data.parquet transform: parse(df, client)
    source_code.py   # initiate_codes, update_codes, dataset_filters, etc.
    base_table.py    # Base Table Gen overrides (optional)
```

## Modules

| Module | Purpose | Called by |
|--------|---------|-----------|
| **parser.py** | Transform data when building data.parquet | `apply_client_specific()` in common/parser.py |
| **source_code.py** | Source code validation: initiate, update, dataset filters | `fix_client_codes()`, `update_client_codes()`, `add_dataset_filters()` in common/source_code_validation.py |
| **base_table.py** | Base Table Gen overrides (skip QA, transforms, etc.) | `apply_client_*()` in common/base_table_clients.py |

## Dispatch

- **common/clients_loader.py** – Dynamic import: `clients.{client}.parser`, `clients.{client}.source_code`, `clients.{client}.base_table`
- Handles case variants (WFP/wfp, NJH/njh)
- Returns `None` if client module not found (no crash)

## Adding a New Client

1. Create `clients/{Client}/` folder
2. Add `__init__.py` (can be empty)
3. Add `parser.py` with `def parse(df, client): return df` (or custom logic)
4. Add `source_code.py` with `initiate_codes`, `update_codes`, `dataset_filters` as needed
5. Add `base_table.py` with any Base Table Gen overrides as needed

## CI/CD Build

For client X:
- Include: `common/` (all) + `clients/X/` + `schema/X/`
- Exclude: other `clients/{Y}/` folders
- Result: minimal artifact for that client
