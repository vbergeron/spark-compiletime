package spark.compiletime.encoders

import scala.quoted.*
import scala.deriving.*
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.*
import org.apache.spark.sql.types.*
import org.apache.spark.unsafe.types.*
import org.apache.spark.sql.Row
import scala.reflect.ClassTag
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.OuterScopes
import spark.compiletime.utils

// A reimplementation of the very special derivation in ScalaReflection.encoderFor
def encoderForImpl[A](using Quotes, Type[A]): Expr[AgnosticEncoder[?]] = {
  import quotes.reflect.*

  val productMirror = Expr.summon[Mirror.ProductOf[A]]

  Type.of[A] match
    case '[Null]              => '{ NullEncoder }
    case '[Boolean]           => '{ PrimitiveBooleanEncoder }
    case '[Byte]              => '{ PrimitiveByteEncoder }
    case '[Short]             => '{ PrimitiveShortEncoder }
    case '[Int]               => '{ PrimitiveIntEncoder }
    case '[Long]              => '{ PrimitiveLongEncoder }
    case '[Float]             => '{ PrimitiveFloatEncoder }
    case '[Double]            => '{ PrimitiveDoubleEncoder }
    case '[java.lang.Boolean] => '{ BoxedBooleanEncoder }
    case '[java.lang.Byte]    => '{ BoxedByteEncoder }
    case '[java.lang.Short]   => '{ BoxedShortEncoder }
    case '[java.lang.Integer] => '{ BoxedIntEncoder }
    case '[java.lang.Long]    => '{ BoxedLongEncoder }
    case '[java.lang.Float]   => '{ BoxedFloatEncoder }
    case '[java.lang.Double]  => '{ BoxedDoubleEncoder }
    case '[Array[Byte]]       => '{ BinaryEncoder }

    // Enums
    case '[t] if utils.subtypeOf[t, java.lang.Enum[?]] =>
      '{ JavaEnumEncoder(ClassTag(${ utils.classOf[A] })) }

    case '[t] if utils.subtypeOf[t, Enumeration#Value] =>
      utils.parentTypeOf[t] match
        case '[parent] =>
          '{ ScalaEnumEncoder(${ utils.classOf[parent] }, ${ utils.classTagOf[t] }) }

    // Leaf encoders
    case '[String]                  => '{ StringEncoder }
    case '[Decimal]                 => '{ DEFAULT_SPARK_DECIMAL_ENCODER }
    case '[BigDecimal]              => '{ DEFAULT_SCALA_DECIMAL_ENCODER }
    case '[java.math.BigDecimal]    => '{ DEFAULT_JAVA_DECIMAL_ENCODER }
    case '[BigInt]                  => '{ ScalaBigIntEncoder }
    case '[java.math.BigInteger]    => '{ JavaBigIntEncoder }
    case '[CalendarInterval]        => '{ CalendarIntervalEncoder }
    case '[java.time.Duration]      => '{ DayTimeIntervalEncoder }
    case '[java.time.Period]        => '{ YearMonthIntervalEncoder }
    case '[java.sql.Date]           => '{ STRICT_DATE_ENCODER }
    case '[java.time.LocalDate]     => '{ STRICT_LOCAL_DATE_ENCODER }
    case '[java.sql.Timestamp]      => '{ STRICT_TIMESTAMP_ENCODER }
    case '[java.time.Instant]       => '{ STRICT_INSTANT_ENCODER }
    case '[java.time.LocalDateTime] => '{ LocalDateTimeEncoder }
    case '[Row]                     => '{ UnboundRowEncoder }

    // Complex encoders
    case '[Option[t]] =>
      '{
        val encoder = ${ encoderForImpl[t] }
        OptionEncoder(encoder)
      }

    case '[Array[t]] =>
      '{
        val encoder = ${ encoderForImpl[t] }
        ArrayEncoder(encoder, encoder.nullable)
      }

    case '[t] if utils.subtypeOf[t, Seq[?]] =>
      TypeRepr.of[t].typeArgs.head.asType match
        case '[e] =>
          val companion = TypeRepr.of[t].typeSymbol.companionModule
          if companion.methodMember("newBuilder").nonEmpty
          then
            '{
              val encoder = ${ encoderForImpl[e] }
              IterableEncoder(${ utils.classTagOf[A] }, encoder, encoder.nullable, false)
            }
          else
            '{
              val encoder = ${ encoderForImpl[e] }
              IterableEncoder(${ utils.classTagOf[Seq[?]] }, encoder, encoder.nullable, false)
            }

    case '[t] if utils.subtypeOf[t, Set[?]] =>
      TypeRepr.of[t].typeArgs.head.asType match
        case '[e] =>
          val companion = TypeRepr.of[t].typeSymbol.companionModule
          if companion.methodMember("newBuilder").nonEmpty
          then
            '{
              val encoder = ${ encoderForImpl[e] }
              IterableEncoder(${ utils.classTagOf[A] }, encoder, encoder.nullable, false)
            }
          else
            '{
              val encoder = ${ encoderForImpl[e] }
              IterableEncoder(${ utils.classTagOf[Set[?]] }, encoder, encoder.nullable, false)
            }

    case '[Map[k, v]] =>
      '{
        val key   = ${ encoderForImpl[k] }
        val value = ${ encoderForImpl[v] }
        MapEncoder(${ utils.classTagOf[A] }, key, value, value.nullable)
      }

    case '[t] if productMirror.isDefined =>
      productMirror.get match
        case '{ $m: Mirror.ProductOf[A] { type MirroredElemLabels = labels; type MirroredElemTypes = types } } =>
          val labels = utils.stringsFromTuple[labels]
          val types  = utils.typesFromTuple[types]

          types.foreach:
            case '[e] =>
              if TypeRepr.of[e] =:= TypeRepr.of[t]
              then report.errorAndAbort(s"circular dependency: ${Type.show[A]} contains itself")

          val params = (labels zip types).map:
            case label -> '[t] =>
              '{
                val encoder = ${ encoderForImpl[t] }
                EncoderField(${ Expr(label) }, encoder, encoder.nullable, Metadata.empty)
              }
            case unreachable   =>
              report.errorAndAbort(s"Unexpected type ${Type.show[A]}")
          '{
            ProductEncoder(
              ${ utils.classTagOf[A] },
              ${ Expr.ofList(params) },
              Option(OuterScopes.getOuterScope(${ utils.classOf[A] }))
            )
          }

    case unexpected =>
      report.errorAndAbort(s"Unexpected type ${Type.show[A]}")
}

private[compiletime] inline def agnosticEncoderOf[A]: AgnosticEncoder[?] =
  ${ encoderForImpl[A] }

inline def encoderOf[A] =
  ExpressionEncoder[A](agnosticEncoderOf[A].asInstanceOf)
