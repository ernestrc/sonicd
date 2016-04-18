# Sonicd [![Build Status](https://travis-ci.org/ernestrc/sonicd.svg?branch=master)](https://travis-ci.org/ernestrc/sonicd)

# The problem
- If you're using Akka Streams for stream processing instead of other data streaming frameworks (like Storm), you'll be probably missing a data sourcing component (like Spout).
- Programatic big data access it's highly dominated by the JVM, aside from some highly opinionated 3rd party libraries, and requires a lot of configuration on the client side that makes it hard or sometimes impossible to use from other environments (browser, nodejs, etc)
- Java CLI's suck. Waiting 5s for the JVM every time you want to run `hive -e "show tables"` it's a complete loss of time.

# The solution
Sonicd is a data streaming gateway that abstracts over data source connectors and provides a unified api/protocol to stream data over WebSockets or over TCP (using one of the client libs). It also provides a CLI (which uses the Rust lib) to run ad hoc queries.

# Supported Sources
- Any database with a JDBC driver implementation (tested with Hive, Presto, Redshift, H2, MySQL, PostgreSQL).
- Zuora objects via ZOQL (Zuora Object Query Language).
- Kafka (WIP)

# CLI
 Install multirust with `curl -sf https://raw.githubusercontent.com/brson/multirust/master/blastoff.sh | sh` or check [https://github.com/brson/multirust](https://github.com/brson/multirust) - the CLI uses several compiler plugins that are only enabled in the nightly version of rust, so we'll need a specific nightly version. Execute `multirust override nightly-2015-11-14` in the root folder and then compile the sources with `cargo build --release`. You will find the binary inside the `target/release` folder.

# Examples
Check [examples](examples) folder. For an example in Rust check the [cli](cli).

# Client libraries
- NodeJS
- Rust
- Scala/Java (Akka Streams)

# Contribute
If you would like to contribute to the project, please fork the project, include your changes and submit a pull request back to the main repository.

# License
MIT License 
