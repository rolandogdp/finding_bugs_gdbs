## Introduction
  GraphGenie fork to focus on different type of bug detection.



## Environment Requirement

The code has been tested running under Python 3.8.10. The OS is 20.04.2 LTS Ubuntu Linux 64-bit distribution.


## Usage

Config graphgenie.ini first and then start the testing:

If you test Neo4j, simply run the main.py

```
./main.py
```

For other databases, you need to first initialize the dataset and specify

```
node_labels, edge_labels, node_properties, connectivity_matrix
```

in main.py line 332, or implement your own schema scanner (should be similar to Neo4j one).

Detected bugs can be found in `./bug.log` (logic bugs or performance issues) or `./exception.log` (internal errors). The `./testing.log` records all executed queries.




**Neo4j**

```
apt install openjdk-17-jdk;
cd dbs;
wget https://dist.neo4j.org/neo4j-community-5.11.0-unix.tar.gz;
tar -xvf neo4j-community-5.11.0-unix.tar.gz;
git clone https://github.com/neo4j-graph-examples/recommendations.git;
cd neo4j-community-5.11.0;
./bin/neo4j-admin database load --from-stdin --overwrite-destination=true neo4j < ../recommendations/data/recommendations-50.dump;
echo "dbms.transaction.timeout=30s" >> ./conf/neo4j.conf
./bin/neo4j start
```

Then please connect to the server to config your password
(the default username:neo4j password:neo4j)
(password is 12344321 in our default setting)

```
./bin/cypher-shell
```
