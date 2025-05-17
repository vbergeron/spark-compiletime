package spark.encoders

case class User(
    friends: Seq[String]
)

val encoder = macros.encoderOf[User]

@main
def run =
  encoder.schema.printTreeString
