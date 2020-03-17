# databasewrapper-sql2o
A light-weight ORM kind of wrapper over the SQL2O

This wrapper allows you to Query database. a simple DSL syntax,
 supports multiple databases, integrates well with Java8.
 

## sql2o 
Sql2o is a small java library, with the purpose of making database interaction easy.
When fetching data from the database, the ResultSet will automatically be filled 
into your POJO objects. 

Kind of like an ORM, but without the SQL generation capabilities.

### Performance of SELECT

Execute 1000 SELECT statements against a DB and map the data returned to a POJO.
Code is available [here](https://github.com/aaberg/sql2o/blob/master/core/src/test/java/org/sql2o/performance/PojoPerformanceTest.java).

Method                                                              | Duration               |
------------------------------------------------------------------- | ---------------------- |
Hand coded <code>ResultSet</code>                                   | 60ms                   |
Sql2o                                                               | 75ms (25% slower)      |
[Apache DbUtils](http://commons.apache.org/proper/commons-dbutils/) | 98ms (63% slower)      |
[JDBI](http://jdbi.org/)                                            | 197ms (228% slower)    |
[MyBatis](http://mybatis.github.io/mybatis-3/)                      | 293ms (388% slower)    |
[jOOQ](http://www.jooq.org)                                         | 447ms (645% slower)    |
[Hibernate](http://hibernate.org/)                                  | 494ms (723% slower)    |
[Spring JdbcTemplate](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/jdbc.html) | 636ms (960% slower) |


### Credits 
Inspired by the project https://github.com/biezhi/anima

Baseline is written based on that project and fixed few issues and maintaining seperately