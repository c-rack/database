# Database

This library combines the best libraries to interact with databases in Java:

* [sql2o](https://www.sql2o.org/)
* [HikariCP](https://github.com/brettwooldridge/HikariCP)
* [Apache Calcite](https://calcite.apache.org/)

in order to:

* protect against SQL injection attacks

  * by disallowing multiple statements in one query
  * by disallowing functions like SLEEP, BENCHMARK, etc.

* enforce best practice and simplify developer experience

  * by always using a connection pool
  * by providing an optional asynchronous slow query callback on nanosecond accuracy
  * by combining 3 libraries into 1, with full access to the underlying libraries

Licensed under AGPL.
Perpetual commercial license available.
