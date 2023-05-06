# Flink Kubernetes CDC Connector

## Usage
```sql
CREATE TABLE pods (
    _id STRING, // must be declared
    namespace STRING,
    name STRING,
    phase STRING,
    PRIMARY KEY(_id) NOT ENFORCED
) WITH (
    'connector' = 'k8s'
);
```