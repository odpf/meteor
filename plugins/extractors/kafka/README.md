# kafka

## Usage

```yaml
source:
  name: kafka
  config:
    broker: "localhost:9092"
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `broker` | `string` | `localhost:9092` | Kafka broker's host | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `my-topic` |
| `resource.name` | `my-topic` |
| `resource.service` | `kafka` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
