package com.lollypop.runtime.instructions.expressions

import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.models._
import com.lollypop.language.{dieUnsupportedConversion, dieUnsupportedType}
import com.lollypop.runtime.LollypopVM.execute
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassInstanceSugar
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.{QMap, Row, RowCollection}
import com.lollypop.runtime.instructions.RuntimeInstruction
import com.lollypop.runtime.instructions.functions.{AnonymousFunction, ArgumentBlock}
import com.lollypop.runtime.{LollypopVM, Scope, safeCast}
import com.lollypop.util.JSONSupport.JSONProductConversion
import com.lollypop.util.JVMSupport._
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost
import spray.json.JsArray

import java.io.File
import java.nio.CharBuffer
import java.util.Date
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
 * Represents a run-time Expression
 */
trait RuntimeExpression extends Expression with RuntimeInstruction {

  def evaluate()(implicit scope: Scope): Any

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = evaluate() ~> { v => (scope, IOCost.empty, v) }

  def processInternalOps(host: Expression, args: List[Expression])(implicit scope: Scope): Any = {
    val instance = LollypopVM.execute(scope, host)._3
    val values = LollypopVM.evaluate(scope, args)
    val tuple = this
    instance match {
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
      case fx: LambdaFunction => LollypopVM.execute(scope, fx.call(args))._3
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
      case inst => inst.invokeMethod(name = "apply", params = args)
    }
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
    def asAny(implicit scope: Scope): Option[Any] = Option(execute(scope, getValue)._3)

    @inline
    def asArray(implicit scope: Scope): Option[Array[_]] = {
      @tailrec
      def recurse(value: Any): Array[_] = value match {
        case seq: Seq[Any] => recurse(seq.toArray)
        case array if array.getClass.isArray => array.asInstanceOf[Array[_]]
        case s: String => s.toCharArray
        case x => dieUnsupportedConversion(x, typeName = "Array")
      }

      Option(recurse(execute(scope, getValue)._3))
    }

    @inline def asBlob(implicit scope: Scope): Option[java.sql.Blob] = Option(execute(scope, getValue)._3).map(BlobType.convert)

    @inline def asBoolean(implicit scope: Scope): Option[Boolean] = Option(execute(scope, getValue)._3).map(BooleanType.convert)

    @inline def asByteArray(implicit scope: Scope): Option[Array[Byte]] = Option(execute(scope, getValue)._3).map(VarBinaryType.convert)

    @inline def asChar(implicit scope: Scope): Option[Char] = Option(execute(scope, getValue)._3).map(CharType.convert)

    @inline def asClob(implicit scope: Scope): Option[ICLOB] = Option(execute(scope, getValue)._3).map(ClobType.convert)

    @inline def asCharArray(implicit scope: Scope): Option[Array[Char]] = Option(execute(scope, getValue)._3).map {
      case c: Array[Char] => c
      case c: CharBuffer => c.array()
      case c: Range => c.toArray.map(_.toChar)
      case s: String => s.toCharArray
      case x => dieUnsupportedConversion(x, typeName = "char[]")
    }

    @inline def asDateTime(implicit scope: Scope): Option[Date] = Option(execute(scope, getValue)._3).map(DateTimeType.convert)

    @inline
    def asDictionary(implicit scope: Scope): Option[mutable.Map[String, _]] = {
      Option(execute(scope, getValue)._3.normalize).map {
        case m: QMap[_, _] => mutable.LinkedHashMap(m.toSeq.map { case (k, v) => String.valueOf(k) -> v }: _*)
        case x => dieUnsupportedType(x)
      }
    }

    @inline
    def asDictionaryOf[A](implicit scope: Scope): Option[mutable.Map[String, A]] = {
      Option(execute(scope, getValue)._3.normalize).map {
        case m: QMap[_, _] => mutable.LinkedHashMap[String, A](m.toSeq.flatMap { case (k, v) => safeCast[A](v).map(vv => String.valueOf(k) -> vv) }: _*)
        case x => dieUnsupportedType(x)
      }
    }

    @inline def asDouble(implicit scope: Scope): Option[Double] = Option(execute(scope, getValue)._3).map(Float64Type.convert)

    @inline
    def asFile(implicit scope: Scope): Option[File] = {

      @tailrec
      def recurse(value: Any): File = value match {
        case f: File => f
        case c: Char => recurse(String.valueOf(c))
        case c: Character => recurse(String.valueOf(c))
        case s: String if s.startsWith(File.separator) => new File(s).getCanonicalFile
        case s: String => (scope.getUniverse.system.currentDirectory.map(_ / s) || new File(s)).getCanonicalFile
        case x => dieUnsupportedConversion(x, typeName = "file")
      }

      Option(execute(scope, getValue)._3).map(recurse)
    }

    @inline
    def asAnonymousFunction(implicit scope: Scope): Option[AnonymousFunction] = {
      Option(execute(scope, getValue)._3).collect { case af: AnonymousFunction => af }
    }

    @inline def asFloat(implicit scope: Scope): Option[Float] = Option(execute(scope, getValue)._3).map(Float32Type.convert)

    @inline def asInt8(implicit scope: Scope): Option[Byte] = Option(execute(scope, getValue)._3).map(Int8Type.convert)

    @inline def asInt16(implicit scope: Scope): Option[Short] = Option(execute(scope, getValue)._3).map(Int16Type.convert)

    @inline def asInt32(implicit scope: Scope): Option[Int] = Option(execute(scope, getValue)._3).map(Int32Type.convert)

    @inline def asInt64(implicit scope: Scope): Option[Long] = Option(execute(scope, getValue)._3).map(Int64Type.convert)

    @inline def asInterval(implicit scope: Scope): Option[FiniteDuration] = Option(execute(scope, getValue)._3).map(IntervalType.convert)

    @inline
    def asJsArray(implicit scope: Scope): Option[JsArray] = {
      def recurse(value: Any): JsArray = value match {
        case array: Array[_] => JsArray(array.map(_.toJsValue): _*)
        case seq: Seq[_] => JsArray(seq.map(_.toJsValue): _*)
        case array: JsArray => array
        case x => dieUnsupportedConversion(x, typeName = "JsArray")
      }

      Option(recurse(execute(scope, getValue)._3))
    }

    @inline def asNumeric(implicit scope: Scope): Option[Number] = Option(execute(scope, getValue)._3).map(NumericType.convert)

    @inline def asString(implicit scope: Scope): Option[String] = Option(execute(scope, getValue)._3).map(StringType.convert)

    @inline def get(implicit scope: Scope): Option[Any] = Option(execute(scope, getValue)._3)

    @inline def toTextString(implicit scope: Scope): Option[String] = asString

    private def getValue: Expression = expression match {
      case ArgumentBlock(List(arg)) => arg
      case expr => expr
    }

  }

}
