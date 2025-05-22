package spark.compiletime

import org.apache.spark.sql.SparkSession

class SimpleTest extends munit.FunSuite {

  val user = table("create table user (name string, age int not null)")

  val post = table("create table post (author string, content string, tags array<string>)")

  val domain = catalog(user, post)

  val query = domain.sql("select * from user join post on (user.name = post.author)")

  // kind of a devoid test case since we are only asserting compilability
  test("SparkSession compiletime test".ignore) {
    val spark: SparkSession = ???

    // we can use the generated schema
    spark.read.schema(user.schema)

    spark.sql(user.query + "using json") // + options

    spark.sql(query)
  }

  test("Consistency of schemas") {
    assertEquals(user.schema, encoders.encoderOf[User].schema)
    assertEquals(post.schema, encoders.encoderOf[Post].schema)
  }

  // This does not compiles.
  // Please uncomment when function support is reached
  // domain.sql("select upper(name) from user")

}
