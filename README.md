dataRelay: A data relay from Kafka to target address


Cassandra version (1.x, 2.x only, not capatible with 3.x)



Options
---------------

- `server`/`s` : The comma separated list of server could be brokers in the Kafka cluster or spark address.
- `topic`/`t` : The topic to kafka produce.
- `dest`/`d` : Destination to `kafka`, `spark`, `cassandra` and `stdout`.
- `pipe`/`p`: Using pipe mode to forward data. usage `cat data.txt | linkerConnector -p true`
- `--help` : Display detail command help.


Cassandra Data Schema
---------------

#### Run Cassandra 2.2.5 with docker

```
docker run --name cassandra-dap -p 9042:9042 -p 7000:7000 -p 7001:7001 -p 7199:7199 -d cassandra:2.2.5
```

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

Installation and Usage
---------------
#### Installation:

- `git clone https://github.com/LinkerNetworks/dataRelay `
- install Golang
- `cd $GOPATH/src/gihub.com/LinkerNetworks/dataRelay`
- `go build and go install`

### Usage:

#### Relay Kafka data to Cassandra

```
dataRelay -s=127.0.0.1:9092 -t=test -d=cassandra
```

#### Relay Kafka data to Stdout

```
dataRelay -s=127.0.0.1:9092 -t=test 
```