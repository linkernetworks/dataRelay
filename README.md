dataRelay


Cassandra version (1.x, 2.x only, not capatible with 3.x)

### Run Cassandra 2.2.5 with docker

```
docker run --name cassandra-dap -p 9042:9042 -p 7000:7000 -p 7001:7001 -p 7199:7199 -d cassandra:2.2.5
```



### Cassandra Data Schema

#### Keyspace 

```
CREATE KEYSPACE YOUR_KEYSPACE WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};
```

#### Table

```
CREATE TABLE test_topic (
key text, 
value text, 
PRIMARY KEY (value));
```