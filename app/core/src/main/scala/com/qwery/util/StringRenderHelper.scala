package com.qwery.util

import com.qwery.implicits.MagicImplicits
import com.qwery.language.models.Instruction
import com.qwery.runtime.plastics.Tuples.tupleToSeq
import com.qwery.runtime.datatypes.{DataType, StringType}
import com.qwery.runtime.devices.{QMap, Row, RowCollection}
import com.qwery.util.JVMSupport.NormalizeAny
import qwery.io.RowIDRange
import qwery.lang.BitArray
import spray.json.JsValue

import java.util.UUID
import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration

/**
 * String Render Helper
 */
object StringRenderHelper {

  def toByteArrayString(bytes: Array[Byte], isPretty: Boolean = false): String = {
    def isPrintable(b: Byte) = b >= 32 && b <= 126

    val list = bytes.foldLeft[List[String]](Nil) {
      case (agg, b) if b == 8 => "'\\b'" :: agg
      case (agg, b) if b == 9 => "'\\t'" :: agg
      case (agg, b) if b == 10 => "'\\n'" :: agg
      case (agg, b) if b == 13 => "'\\r'" :: agg
      case (agg, b) if isPretty && isPrintable(b) && agg.headOption.exists(_.startsWith("\"")) =>
        val prev = agg.head.drop(1).dropRight(1)
        "\"" + prev + b.toChar + "\"" :: agg.tail
      case (agg, b) if isPretty && isPrintable(b) => "\"" + b.toChar + "\"" :: agg
      //case (agg, b) if isPrintable(b) => "'" + b.toChar + "'" :: agg
      case (agg, b) => f"0x$b%02x" :: agg
    }
    list.reverse.mkString("[", ", ", "]")
  }

  def toClassString(`class`: Class[_]): String = `class` match {
    case c if c.isArray => toClassString(c.getComponentType) + "[]"
    case c => c.getName
  }

  def toRowString(row: Row): String = (row.toMap ++ Map("__id" -> row.id)).renderAsJson

  def toProductString(product: Product): String = {
    product match {
      case None => "None"
      case b: BitArray => b.toString
      case r: RowCollection => r.renderAsJson
      case r: RowIDRange => r.toList.mkString("[", ", ", "]")
      case d: DataType => d.toSQL
      case i: Instruction => i.toSQL
      case p =>
        p.getClass.getSimpleName + p.productElementNames.toSeq.zip(p.productIterator)
          .map { case (a, b) => s"$a=${b.renderAsJson}" }
          .mkString("(", ", ", ")")
    }

  }

  /**
   * String Renderer
   * @param item the [[Any value]] to render as a string
   */
  final implicit class StringRenderer(val item: Any) extends AnyVal {

    @inline def render: String = _render(quotedLiterals = false)

    @tailrec
    def renderAsJson: String = item match {
      case _: Seq[_] => _render(quotedLiterals = true)
      case p: Product if p.getClass.getName.startsWith("scala.Tuple") => p._render(quotedLiterals = true)
      case p: Product => Map(p.getClass.getDeclaredFields.map(_.getName) zip p.productIterator: _*).renderAsJson
      case _ => _render(quotedLiterals = true)
    }

    @inline def renderFriendly: String = item match {
      case _: Seq[_] => _render(quotedLiterals = false, f = collapse)
      case p: Product if p.getClass.getName.startsWith("scala.Tuple") => p._render(quotedLiterals = true)
      case p: Product => toProductString(p)
      case _ => _render(quotedLiterals = false, f = collapse)
    }

    @inline def renderTable: String = item match {
      case null | None => ""
      case Some(v) => s"Some(${v.renderTable})"
      case _: Seq[_] => _render(quotedLiterals = false, f = collapse)
      case p: Product if p.getClass.getName.startsWith("scala.Tuple") => p._render(quotedLiterals = true)
      case p: Product => toProductString(p)
      case _ => _render(quotedLiterals = false, f = collapse)
    }

    @inline def renderPretty: String = {
      import spray.json._
      import DefaultJsonProtocol._
      item match {
        case a: Array[_] => a.renderFriendly
        case j: JsValue => j.prettyPrint
        case r: Row => StringRenderHelper.toRowString(r)
        case r: RowCollection => r.toList.map(_.toMap.map { case (k, v) => k -> StringType.convert(v) }).toJson.compactPrint
        case s: Seq[_] => s.renderFriendly
        case p: Product => toProductString(p)
        case x => x.renderFriendly
      }
    }

    private def collapse(s: String): String = {
      s.replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    }

    def _render(quotedLiterals: Boolean, f: String => String = s => s): String = {
      def recurse(value: Any, ql: Boolean): String = {
        value.normalize.asInstanceOf[AnyRef].normalizeJava match {
          case None => if (!quotedLiterals) "None" else ""
          case Some(v) => s"Some(${recurse(v, ql)})"
          //case a: Array[Byte] => toByteArrayString(a)
          case a: Array[Byte] => new String(a)
          case a: Array[_] => recurse(a.toSeq, ql)
          case b: java.sql.Blob => recurse(b.getBinaryStream.readAllBytes(), ql)
          case c: java.sql.Clob => recurse(c.getAsciiStream.readAllBytes(), ql)
          case c: Character => if (ql) s"'$c'" else String.valueOf(c)
          case c: Class[_] => toClassString(c) ~> { name => if (name.contains('.')) s"`$name`" else name }
          case d: java.util.Date => recurse(DateHelper.format(d), ql)
          case d: FiniteDuration => recurse(d.toString, ql)
          case j: JsValue => j.compactPrint
          case r: Row => toRowString(r)
          case m: QMap[_, _] => m.map { case (k, v) => s"${recurse(k, ql = true)}: ${recurse(v, ql = true)}" }.mkString("{", ", ", "}")
          case s: Seq[_] => s.map(recurse(_, ql = true)).mkString("[", ", ", "]")
          case s: Set[_] => s.map(recurse(_, ql = true)).mkString("(", ", ", ")")
          case s: java.sql.SQLXML => recurse(s.getBinaryStream.readAllBytes(), ql)
          case s: String => escapeString(s, ql)
          case u: UUID => recurse(u.toString, ql)
          case z =>
            // it is a tuple?
            tupleToSeq(z) match {
              case Some(list: Seq[_]) => list.map(recurse(_, ql)).mkString("(", ", ", ")")
              case None =>
                // it is a Product?
                z match {
                  case p: Product => toProductString(p)
                  case x => String.valueOf(x)
                }
            }
        }
      }

      f(recurse(item, quotedLiterals))
    }

    private def escapeString(text: String, ql: Boolean): String = {
      if (ql) {
        if (text.contains("\"\"\"")) s"'''$text'''"
        else if (text.contains('"')) s"'$text'"
        else s"\"$text\""
      } else text
    }

  }

}
