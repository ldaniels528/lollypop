package com.lollypop.util

import com.lollypop.language.models.Expression
import com.lollypop.language.models.Expression.implicits._
import com.lollypop.language.{Template, dieUnsupportedType}
import com.lollypop.runtime.devices.{QMap, Row, RowCollection}
import com.lollypop.runtime.instructions.expressions.ArrayLiteral
import com.lollypop.util.JVMSupport.NormalizeAny
import com.lollypop.util.StringRenderHelper.StringRenderer
import lollypop.lang.Null
import spray.json._

import java.util.{Date, UUID}
import scala.collection.mutable

/**
 * JSON Support Capability
 * @author lawrence.daniels@gmail.com
 */
object JSONSupport {

  /**
   * JSON String Conversion
   * @param jsonString the JSON string
   */
  final implicit class JSONStringConversion(val jsonString: String) extends AnyVal {
    @inline def fromJSON[T](implicit reader: JsonReader[T]): T = jsonString.parseJson.convertTo[T]

    @inline def parseJSON: JsValue = jsonString.parseJson
  }

  /**
   * JsValue Conversion (Spray)
   * @param jsValue the [[JsValue JSON value]]
   */
  final implicit class JsValueConversion(val jsValue: JsValue) extends AnyVal {
    @inline
    def unwrapJSON: Any = jsValue match {
      case js: JsArray => js.elements.map(_.unwrapJSON)
      case JsBoolean(value) => value
      case JsNull => null
      case JsNumber(value) => value
      case js: JsObject => js.fields map { case (name, jsValue) => name -> jsValue.unwrapJSON }
      case JsString(value) => value
      case x => dieUnsupportedType(x)
    }

    @inline
    def toExpression: Expression = jsValue match {
      case js: JsArray => ArrayLiteral(value = js.elements.map(_.toExpression).toList)
      case JsBoolean(value) => value
      case JsNull => Null()
      case JsNumber(value) => value.toDouble
      case js: JsObject => js.fields map { case (name, jsValue) => name -> jsValue.unwrapJSON.v }
      case JsString(value) => value
      case x => dieUnsupportedType(x)
    }
  }

  final implicit class AnyToSprayJsConversion(val value: Any) extends AnyVal {

    def toSprayJs: JsValue = {
      import com.lollypop.runtime.ModelsJsonProtocol._

      def recurse(value: Any): JsValue = value match {
        case None | null => JsNull
        case Some(v) => recurse(v)
        case v: Array[_] => JsArray(v.map(recurse): _*)
        case v: Seq[_] => JsArray(v.map(recurse): _*)
        case m: QMap[String, _] => JsObject(fields = m.toMap.map { case (k, v) => k -> recurse(v) })
        case v: BigDecimal => v.toJson
        case v: BigInt => v.toJson
        case v: RowCollection => v.toMapGraph.toJson
        case v: Boolean => v.toJson
        case v: Byte => v.toJson
        case v: Date => DateHelper.format(v).toJson
        case v: Class[_] => s"""classOf("${v.getName}")""".toJson
        case v: Double => v.toJson
        case v: Float => v.toJson
        case v: Int => v.toJson
        case v: Long => v.toJson
        case n: Number => JsNumber(n.doubleValue())
        case v: Short => v.toJson
        case v: String => v.toJson
        case v: Template => v.toString.toJson
        case v => v.render.toJson
        case v => dieUnsupportedType(v)
      }

      recurse(value.normalize)
    }
  }

  /**
   * JSON Product Conversion
   * @param value the value to convert
   */
  final implicit class JSONProductConversion[T](val value: T) extends AnyVal {

    @inline
    def toJsValue: JsValue = value.normalize.asInstanceOf[AnyRef] match {
      case Some(value) => value.toJsValue
      case None | null => JsNull
      case a: Array[_] => JsArray(a.map(_.toJsValue): _*)
      case a: Seq[_] => JsArray(a.map(_.toJsValue): _*)
      case b: RowCollection => JsArray(b.toList.map(_.toMap).map(_.toJsValue): _*)
      case b: java.lang.Boolean => JsBoolean(b)
      case c: Class[_] => JsString(c.getName)
      case d: Date => JsString(DateHelper.format(d))
      case j: JsValue => j
      case r: Row => JsObject(r.toMap.map { case (k, v) => k -> v.toJsValue })
      case m: QMap[_, _] => JsObject(m.toMap.map { case (k, v) => k.toString -> v.toJsValue })
      case n: Number => if (n.longValue() == n.doubleValue()) JsNumber(n.longValue()) else JsNumber(n.doubleValue())
      case p: Product =>
        val tuples = (p.productElementNames zip p.productIterator.map(_.toJsValue)).toList
        JsObject(Map(tuples ::: ("_class" -> JsString(p.getClass.getName)) :: Nil: _*))
      case s: String => JsString(s)
      case s: mutable.StringBuilder => JsString(s.toString)
      case s: StringBuffer => JsString(s.toString)
      case u: UUID => JsString(u.toString)
      case _ =>
        // otherwise, just assume it's a POJO
        JsObject(Map((for {
          m <- value.getClass.getMethods if m.getParameterTypes.isEmpty && m.getName.startsWith("get")
          name = m.getName.drop(3) match {
            case "Class" => "_class"
            case other => other.head.toLower + other.tail
          }
          jsValue = m.invoke(value).toJsValue
        } yield name -> jsValue): _*))
    }

    @inline def toJSON(implicit writer: JsonWriter[T]): String = value.toJson.compactPrint

    @inline def toJSONPretty(implicit writer: JsonWriter[T]): String = value.toJson.prettyPrint

  }

}
