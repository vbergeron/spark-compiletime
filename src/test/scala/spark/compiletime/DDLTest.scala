package spark.compiletime

class DDLTest extends munit.FunSuite {

  test("create table") {
    val t = table("CREATE TABLE test (id INT, name STRING)")
    assertEquals(t.db, "default")
    assertEquals(t.name, "test")
    assertEquals(t.schemaString, "id INT,name STRING")
  }

  test("create table with options") {
    val t = table("CREATE TABLE test (id INT, name STRING) USING iceberg TBLPROPERTIES ('prop'='value')")
    assertEquals(t.db, "default")
    assertEquals(t.name, "test")
    assertEquals(t.schemaString, "id INT,name STRING")
  }

  test("create table in specific database") {
    val t = table("CREATE TABLE mydb.test (id INT, name STRING)")
    assertEquals(t.db, "mydb")
    assertEquals(t.name, "test")
    assertEquals(t.schemaString, "id INT,name STRING")
  }

  test("create table with long namespace") {
    val t = table("CREATE TABLE foo.bar.baz.qux.test (id INT, name STRING)")
    assertEquals(t.db, "foo.bar.baz.qux")
    assertEquals(t.name, "test")
    assertEquals(t.schemaString, "id INT,name STRING")
  }

  test("create table as select") {

    val other = table("CREATE TABLE other (id INT, name STRING)")

    val domain = catalog(other)

    val t = domain.add("CREATE TABLE test (SELECT id, name FROM other)")

    // TODO: re-enable once we can get a table from a catalog at compile-time
    // assertEquals(t.db, "default")
    // assertEquals(t.name, "test")
    // assertEquals(t.schemaString, "id INT,name STRING")
  }

  test("replace table") {
    val t = table("CREATE OR REPLACE TABLE test (id INT, name STRING)")
    assertEquals(t.db, "default")
    assertEquals(t.name, "test")
    assertEquals(t.schemaString, "id INT,name STRING")
  }

}
