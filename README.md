# spark-compiletime

A compile-time validation library for Apache Spark SQL that enables type-safe SQL operations, schema validation, and optimized encoders for Scala 3 applications.

## Features

### Compiletime Table and Catalog Definitions

Define your tables with schema information that travels with your code:
- Create table definitions directly from SQL CREATE TABLE statements
- Build catalogs of multiple tables to validate complex queries
- Access the underlying StructType to use in your data sources.

:warning: For now only raw compile time known strings are supported. We plan to add support for interpolators in the future.

```scala
import spark.compiletime.*

val user = table("create table user (name string, age int)")

val post = table("create table post (author string, content string, tags array<string>)")

val catalog = catalog(user, post)

// read with the derived structure
val posts = spark.read.schema(post.schema).json(...)

// alternatively, access the creation query and run it
spark.sql(user.query + "using json ...")
```

### Compile-time SQL Validation

Validate your Spark SQL queries during compilation, eliminating runtime SQL syntax or schema errors
- Catches schema mismatches, type errors, columns typos, join compatibility issues etc...
- Catalyst is _litterally_ running all its analysis against the provided catalog in your scala compiler.
- You get a nice INFO log from your compilator telling you what the resulting plan is looking like, which can be displayed by your IDE.
- Prevents errors before your code even runs!

:warning: Like schema definition, only raw litteral string are supported for now.

```scala
val df = spark.sql(domain.sql("select * from user join post on (user.name = post.author)"))
// Compiles, INFO log
// Project [name#0, age#1, author#2, content#3, tags#4]
// +- Join Inner, (name#0 = author#2)
//    :- SubqueryAlias compiletime.default.user
//    :  +- RelationV2[name#0, age#1] compiletime.default.user user
//    +- SubqueryAlias compiletime.default.post
//       +- RelationV2[author#2, content#3, tags#4] compiletime.default.post post
```

There are some more advanced usage samples in the `./exemples` directory.

### Compile-time Encoder derivation

Generate efficient Spark encoders from a macro, re-enabling derivation of them for Scala 3.
Most of the use case covered by `Encoders.product` should be covered, with some limitations.
- UDT are not supported. Support for annotation based UDT and builtin ones (fomr graphx and mllib) could be possible, but generic registration types would not.
- Scala enumeration types are able to be derived as `AgnosticEncoder` but not as `ExpressionEncoder` (TODO)
- Binding encoders to tables (from the `spark.compiletime.table` function) is not yet supported
