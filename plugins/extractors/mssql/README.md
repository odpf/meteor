# mssql

## Usage

```yaml
source:
  name: mssql
  config:
    connection_url: sqlserver://admin:pass123@localhost:3306/
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `identifier` | `string` | `my-mssql` | Instance alias, the value will be used as part of the urn component | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `mssql::my-mssql/my_database/my_table` |
| `resource.name` | `my_table` |
| `resource.service` | `mssql` |
| `description` | `table description` |
| `profile.total_rows` | `2100` |
| `schema` | [][Column](#column) |

### Column

| Field | Sample Value |
| :---- | :---- |
| `name` | `total_price` |
| `description` | `item's total price` |
| `data_type` | `decimal` |
| `is_nullable` | `true` |
| `length` | `12,2` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
