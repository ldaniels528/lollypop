package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models._
import com.lollypop.language.{dieUnsupportedConversion, dieUnsupportedType}
import com.lollypop.runtime.LollypopVM.implicits.{InstructionExtensions, InstructionSeqExtensions}
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.{QMap, Row, RowCollection}
import com.lollypop.runtime.instructions.RuntimeInstruction
import com.lollypop.runtime.instructions.functions.ArgumentBlock
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassInstanceSugar
import com.lollypop.runtime.{Scope, safeCast}
import com.lollypop.util.JSONSupport.JSONProductConversion
import com.lollypop.util.JVMSupport._
import lollypop.io.IOCost
import spray.json.JsArray

import java.util.Date
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
 * Represents a run-time Expression
 */
trait RuntimeExpression extends Expression with RuntimeInstruction {

  def processInternalOps(host: Expression, args: List[Expression])(implicit scope: Scope): (Scope, IOCost, Any) = {
    val (sa, ca, instance) = host.execute(scope)
    val (sb, cb, values) = args.transform(sa)
    val tuple = this
    val result = instance match {
      // array(5) | ['A' to 'Z'](8, 19)
      case array: Array[_] =>
        values match {
          case List(a) => array(Int32Type.convert(a))
          case List(a, b) => array.slice(Int32Type.convert(a), Int32Type.convert(b))
          case xxx => tuple.dieArgumentMismatch(args = xxx.size, minArgs = 1, maxArgs = 2)
        }
      // device(9875) | device(9500, 9875)
      case rc: RowCollection =>
        values match {
          case List(a) => rc.apply(Int64Type.convert(a))
          case List(a, b) => rc.slice(Int64Type.convert(a), Int64Type.convert(b))
          case xxx => tuple.dieArgumentMismatch(args = xxx.size, minArgs = 1, maxArgs = 2)
        }
      // anonymousFx('a', 'b', 'c')
      case fx: LambdaFunction => fx.call(args).execute(sb)._3
      // ({ symbol: 'T', exchange: 'NYSE', lastSale: 22.77 })(0)
      case row: Row =>
        values match {
          case List(a) => row.fields(Int32Type.convert(a)).value.orNull
          case List(a, b) => row.fields.slice(Int32Type.convert(a), Int32Type.convert(b)).map(_.value.orNull).toArray
          case xxx => tuple.dieArgumentMismatch(args = xxx.size, minArgs = 1, maxArgs = 2)
        }
      // mapping(8)
      case mapping: QMap[_, _] =>
        values match {
          case List(a) => mapping.toSeq(Int32Type.convert(a))
          case xxx => tuple.dieArgumentMismatch(args = xxx.size, minArgs = 1, maxArgs = 2)
        }
      // list(80) | list(1, 5)
      case seq: Seq[_] =>
        values match {
          case List(a) => seq(Int32Type.convert(a))
          case List(a, b) => seq.slice(Int32Type.convert(a), Int32Type.convert(b))
          case xxx => tuple.dieArgumentMismatch(args = xxx.size, minArgs = 1, maxArgs = 2)
        }
      // 'Hello World'(4) | 'Hello World'(2, 6)
      case string: String =>
        values match {
          case List(a) => string.charAt(Int32Type.convert(a))
          case List(a, b) => string.slice(Int32Type.convert(a), Int32Type.convert(b))
          case xxx => tuple.dieArgumentMismatch(args = xxx.size, minArgs = 1, maxArgs = 2)
        }
      // instance.apply(1, 78, 'H')
      case inst => inst.invokeMethod(name = "apply", params = args)(sb)
    }
    (sb, ca ++ cb, result)
  }

}

/**
 * Run-time Expression Companion
 */
object RuntimeExpression {

  /**
   * Rich Expression
   * @param expression the host [[Expression expression]]
   */
  final implicit class RichExpression(val expression: Expression) extends AnyVal {

    @inline
    def getAlias: Option[String] = expression match {
      case NamedExpression(name) => Some(name)
      case expr => expr.alias
      case _ => None
    }

    @inline
    def asAny(implicit scope: Scope): Option[Any] = Option(getValue.execute(scope)._3)

    @inline
    def asArray(implicit scope: Scope): Option[Array[_]] = {
      @tailrec
      def recurse(value: Any): Array[_] = value match {
        case seq: Seq[Any] => recurse(seq.toArray)
        case array if array.getClass.isArray => array.asInstanceOf[Array[_]]
        case s: String => s.toCharArray
        case x => dieUnsupportedConversion(x, typeName = "Array")
      }

      Option(recurse(getValue.execute(scope)._3))
    }

    @inline def asBoolean(implicit scope: Scope): Option[Boolean] = Option(getValue.execute(scope)._3).map(BooleanType.convert)

    @inline def asByteArray(implicit scope: Scope): Option[Array[Byte]] = Option(getValue.execute(scope)._3).map(VarBinaryType.convert)

    @inline def asDateTime(implicit scope: Scope): Option[Date] = Option(getValue.execute(scope)._3).map(DateTimeType.convert)

    @inline
    def asDictionary(implicit scope: Scope): Option[mutable.Map[String, _]] = {
      Option(getValue.execute(scope)._3.normalize).map {
        case m: QMap[_, _] => mutable.LinkedHashMap(m.toSeq.map { case (k, v) => String.valueOf(k) -> v }: _*)
        case x => dieUnsupportedType(x)
      }
    }

    @inline
    def asDictionaryOf[A](implicit scope: Scope): Option[mutable.Map[String, A]] = {
      Option(getValue.execute(scope)._3.normalize).map {
        case m: QMap[_, _] => mutable.LinkedHashMap[String, A](m.toSeq.flatMap { case (k, v) => safeCast[A](v).map(vv => String.valueOf(k) -> vv) }: _*)
        case x => dieUnsupportedType(x)
      }
    }

    @inline def asDouble(implicit scope: Scope): Option[Double] = Option(getValue.execute(scope)._3).map(Float64Type.convert)

    @inline def asInt32(implicit scope: Scope): Option[Int] = Option(getValue.execute(scope)._3).map(Int32Type.convert)

    @inline def asInterval(implicit scope: Scope): Option[FiniteDuration] = Option(getValue.execute(scope)._3).map(DurationType.convert)

    @inline
    def asJsArray(implicit scope: Scope): Option[JsArray] = {
      def recurse(value: Any): JsArray = value match {
        case array: Array[_] => JsArray(array.map(_.toJsValue): _*)
        case seq: Seq[_] => JsArray(seq.map(_.toJsValue): _*)
        case array: JsArray => array
        case x => dieUnsupportedConversion(x, typeName = "JsArray")
      }

      Option(recurse(getValue.execute(scope)._3))
    }

    @inline def asNumeric(implicit scope: Scope): Option[Number] = Option(getValue.execute(scope)._3).map(NumericType.convert)

    @inline def asString(implicit scope: Scope): Option[String] = Option(getValue.execute(scope)._3).map(StringType.convert)

    @inline def get(implicit scope: Scope): Option[Any] = Option(getValue.execute(scope)._3)

    private def getValue: Expression = expression match {
      case ArgumentBlock(List(arg)) => arg
      case expr => expr
    }

  }

}
