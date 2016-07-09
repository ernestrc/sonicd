# Sonicd [![Build Status](https://travis-ci.org/xarxa6/sonicd.svg)](https://travis-ci.org/xarxa6/sonicd)

# The problem
- If you're using Akka Streams for stream processing instead of other data streaming frameworks (like Storm), you'll be probably missing a data sourcing component (like Spout).
- Programatic big data access it's highly dominated by the JVM and it tipically requires a lot of configuration on the client side that makes it hard or sometimes impossible to use from other a non JVM environment (browser, nodejs, etc)
- Java CLI's suck. Waiting 5s for the JVM every time you want to run `hive -e "show tables"` it's a complete waste of time.

# The solution
Sonicd is a data streaming gateway that abstracts over data source connectors and provides a unified api/protocol to stream data over WebSockets or over TCP (using one of the client libs). It also provides a CLI to run ad hoc queries.

# Supported Sources
- **PrestoSource**: non-blocking streaming connector for [Facebook's Presto](https://prestodb.io/).
- **ElasticSearchSource**: non-blocking streaming connector for [ElasticSearch](https://www.elastic.co/products/elasticsearch)
- **JdbcSource**: JDBC connector for any database with a JDBC driver implementation (tested with Hive, Redshift, H2, MySQL, PostgreSQL).
- **ZuoraObjectQueryLanguageSource**: SOAP streaming connector for [ZOQL](https://knowledgecenter.zuora.com/DC_Developers/SOAP_API/M_Zuora_Object_Query_Language).
- **LocalJsonStreamSource**: stream changes in local JSON files to, for example, build rich regression tests of your server-side components.

# Deploy
Check [server/src/main/resources/reference.conf](server/src/main/resources/reference.conf) for a config reference and `docker run -d -v ${CONFIG_DIR}:/etc/sonicd:ro -p 9111:9111 -p 10001:10001 xarxa6/sonicd;`.
If intend to use the JDBC source, then you'll want to add JDBC drivers jars to the classpath:
```bash
docker run -d -p 9111:9111 -p 10001:10001 -v ${CONFIG_DIR}:/etc/sonicd/ -v ${JDBC_DRIVERS}:/var/lib/sonicd/ xarxa6/sonicd
```

# Install CLI
If you have the rust toolchain installed already, then simply `cargo install sonicd`, otherwise install rustup first with `curl https://sh.rustup.rs -sSf | sh` or check [https://www.rustup.rs/](https://www.rustup.rs/).

# Examples
Check [examples](examples) folder. For an example in Rust check the [cli](cli).

# Client libraries
- NodeJS [![npm version](https://badge.fury.io/js/sonicd.svg)](https://badge.fury.io/js/sonicd)
- Scala/Java (Akka Streams) [ ![Bintray](https://api.bintray.com/packages/ernestrc/maven/sonicd-core/images/download.svg)](https://bintray.com/ernestrc/maven/sonicd-core/_latestVersion)
- Rust [![crates.io](http://meritbadge.herokuapp.com/sonicd)](https://crates.io/crates/sonicd)

# Contribute
If you would like to contribute to the project, please fork the project, include your changes and submit a pull request back to the main repository.

# License
MIT License 
