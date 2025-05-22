package spark.compiletime

import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.sql.Row

class EncoderTest extends munit.FunSuite {

  inline def testDerivation[T] = test(s"derive encoder: ${typeName[T]}") {
    encoders.encoderOf[T]
  }

  inline def testDerivationAgnostic[T] = test(s"derive agnostic encoder: ${typeName[T]}") {
    encoders.agnosticEncoderOf[T]
  }

  testDerivation[Null]
  testDerivation[Boolean]
  testDerivation[Byte]
  testDerivation[Short]
  testDerivation[Int]
  testDerivation[Long]
  testDerivation[Float]
  testDerivation[Double]
  testDerivation[java.lang.Boolean]
  testDerivation[java.lang.Byte]
  testDerivation[java.lang.Short]
  testDerivation[java.lang.Integer]
  testDerivation[java.lang.Long]
  testDerivation[java.lang.Float]
  testDerivation[java.lang.Double]
  testDerivation[Array[Byte]]
  testDerivation[String]
  testDerivation[Decimal]
  testDerivation[BigDecimal]
  testDerivation[java.math.BigDecimal]
  testDerivation[BigInt]
  testDerivation[java.math.BigInteger]
  testDerivation[CalendarInterval]
  testDerivation[java.time.Duration]
  testDerivation[java.time.Period]
  testDerivation[java.sql.Date]
  testDerivation[java.time.LocalDate]
  testDerivation[java.sql.Timestamp]
  testDerivation[java.time.LocalDateTime]
  testDerivation[java.time.Instant]

  testDerivationAgnostic[Row]

  type Elem = Int

  testDerivation[Option[Elem]]
  testDerivation[Array[Elem]]
  testDerivation[Vector[Elem]]
  testDerivation[Seq[Elem]]
  testDerivation[List[Elem]]
  testDerivation[Set[Elem]]
  testDerivation[Map[String, Elem]]

  class SeqWithoutNewBuilder[T](underlying: Seq[T]) extends Seq[T]:
    export underlying.*

  class SetWithoutNewBuilder[T](underlying: Set[T]) extends Set[T]:
    export underlying.*

  testDerivation[SeqWithoutNewBuilder[Elem]]
  testDerivation[SetWithoutNewBuilder[Elem]]

  testDerivation[JavaEnum]

  object ScalaEnumeration extends Enumeration {
    val A, B, C = Value
  }

  // TODO: Find why the ExpressionEncoder derivation fails
  testDerivationAgnostic[ScalaEnumeration.Value]

  case class User(name: String, age: Int, letters: Array[ScalaEnumeration.Value])

  testDerivation[User]

  // Should not compile
  // case class Rec(again: Rec)
  // testDerivation[Rec]

}
