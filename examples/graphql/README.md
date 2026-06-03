# pgx GraphQL Practice Setup

End-to-end example: compose a material catalog with nested sizes and colorways
via GraphQL, executed against PostgreSQL.

## Prerequisites

- Docker & Docker Compose
- `pgx` binary built (`cargo build --release`)

## 1. Start infrastructure

```bash
docker compose up -d
```

This starts PostgreSQL with the sample tables pre-loaded.

## 2. Copy the pgx config

```bash
cp -r examples/graphql/pgx/* ~/.pgx/
```

This sets up:
- `~/.pgx/config.toml` — connection profiles + resolver definitions
- `~/.pgx/schema/*.graphql` — GraphQL type definitions
- `~/.pgx/queries/*.graphql` — named queries with selection sets

## 3. Validate the setup

```bash
cargo run -- graphql validate
```

This checks:
- All type references in schema files resolve
- All named queries parse correctly  
- Every non-leaf field has a resolver
- Each resolver SQL is valid (runs EXPLAIN against the DB)

## 4. Run a query

### Basic

```bash
cargo run -- graphql run MaterialFull -V mat_no=M001
```

Returns a nested JSON document with the material, its sizes, and colorways.

### Compact output (single line)

```bash
cargo run -- graphql run MaterialFull -V mat_no=M001 --compact
```

### Save to file

```bash
cargo run -- graphql run MaterialFull -V mat_no=M001 -o result.json
```

### Query different material

```bash
cargo run -- graphql run MaterialFull -V mat_no=M002
```

## Expected output (pretty-printed)

```json
{
  "material": [
    {
      "mat_no": "M001",
      "name": "Premium Cotton Canvas",
      "status": "active",
      "sizes": [
        { "size_code": "S",  "name": "Small" },
        { "size_code": "M",  "name": "Medium" },
        { "size_code": "L",  "name": "Large" },
        { "size_code": "XL", "name": "Extra Large" }
      ],
      "colorways": [
        { "colorway_code": "WH", "name": "White", "hex": "#FFFFFF" },
        { "colorway_code": "BK", "name": "Black", "hex": "#000000" },
        { "colorway_code": "NV", "name": "Navy",  "hex": "#000080" }
      ],
      "features": [
        {
          "feature_name": "Construction",
          "description": "Plain weave",
          "attribute_entries": [
            { "attr_name": "weave_type",   "attr_value": "plain" },
            { "attr_name": "thread_count", "attr_value": "120" }
          ]
        },
        {
          "feature_name": "Care",
          "description": "Standard care instructions",
          "attribute_entries": [
            { "attr_name": "wash",  "attr_value": "30°C" },
            { "attr_name": "bleach", "attr_value": "No" }
          ]
        }
      ]
    }
  ]
}
```

## Structure

```
~/.pgx/
  config.toml           # Connection URL + resolvers
  schema/
    material.graphql    # type Material { ... }
    size.graphql        # type Size { ... }
    colorway.graphql    # type Colorway { ... }
    feature.graphql     # type MaterialFeature { ... }, FeatureAttribute { ... }
  queries/
    MaterialFull.graphql # 3-tier: material -> features -> attributes
```
