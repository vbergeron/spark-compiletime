package spark.compiletime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoder

class SimpleTest extends munit.FunSuite {

  inline def assertNotCompile(inline code: String): Unit =
    assert:
      val errors = compileErrors(code)
      clue(code)
      clue(errors)
      errors.nonEmpty

  val user = table("create table user (name string, age int not null)")

  val post = table("create table post (author string, content string, tags array<string>)")

  val domain = catalog(user, post)

  val foo = catalog.empty.add(user).add(post)

  val domain2 = domain.add("create table user2 as select * from user")

  val query = domain2.sql("select * from user join post on (user.name = post.author)")

  test("Create table errors are checked at compile time") {
    assertNotCompile:
      """table("creat table user (name string, age int not null)")"""
  }

  test("Create view errors are checked at compile time") {
    assertNotCompile:
      """table("create view user as select * from post")"""
  }

  // kind of a devoid test case since we are only asserting compilability
  test("SparkSession compiletime test".ignore) {
    val spark: SparkSession = ???

    // we can use the generated schema
    spark.read.schema(user.schema)

    spark.sql(user.query + "using json") // + options

    spark.sql(query)

    spark.sql(domain)("select * from user join post on (user.name = post.author)")

    case class User(name: String, age: Int)

    given Encoder[User] = encoders.encoderOf[User]

    val ds = spark.createDataset(
      Seq(
        User("Alice", 30),
        User("Bob", 25)
      )
    )

  }

  test("Consistency of schemas") {
    assertEquals(user.schema, encoders.encoderOf[User].schema)
    assertEquals(post.schema, encoders.encoderOf[Post].schema)
  }

}
