package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.TokenStream
import com.lollypop.language.models.Literal
import com.lollypop.runtime.LollypopVM.execute
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.{DataType, Inferences}
import com.lollypop.runtime.instructions.expressions.AnyLiteral.StringInterpolation
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import scala.collection.mutable

/**
 * Represents a literal value (e.g. "Hello")
 * @param value the given value
 */
case class AnyLiteral(value: Any) extends Literal with RuntimeExpression {
  private val _type = Inferences.fromValue(value)

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val result = value match {
      case s: String => s.expandEscapeCharacters.replaceTags
      case x => x
    }
    (scope, IOCost.empty, result)
  }

  override def returnType: DataType = _type

}

object AnyLiteral {

  /**
   * String Interpolation
   * @param text the string for which to interpolate (e.g. '{{ item.symbol }} is {{ item.lastSale }}/share')
   */
  final implicit class StringInterpolation(val text: String) extends AnyVal {

    @inline
    def expandEscapeCharacters: String = {
      val chars = Seq("\\b" -> "\b", "\\f" -> "\f", "\\n" -> "\n", "\\r" -> "\r", "\\t" -> "\t")
      val result = chars.foldLeft(new StringBuilder(text)) { case (agg, (a, b)) =>
        var start = 0
        do {
          start = agg.indexOf(a)
          if (start >= 0) agg.replace(start, start + a.length, b)
        } while (start >= 0)
        agg
      }
      result.toString
    }

    /**
     * Replaces tags with the property values contained within the scope
     * @param scope the implicit [[Scope scope]]
     * @return the text with all tags replaced with their equivalent values
     */
    def replaceTags(implicit scope: Scope): String = {
      val sb = new mutable.StringBuilder(text)
      var last = 0
      var isDone = false

      // replace all tags (e.g. "Hello {{ item.name }}, how are you?")
      do {
        // attempt to find a tag
        val start = sb.indexOf("{{", last)
        val end = sb.indexOf("}}", start)

        isDone = start == -1 || start > end
        if (!isDone) {
          // extract the tag's contents and parse the property (name and possibly property)
          val tag = sb.substring(start + 2, end).trim
          val replacement = scope.getCompiler.nextExpression(TokenStream(tag)).flatMap(v => Option(execute(scope, v)._3)) ?? scope.apply(tag)
          sb.replace(start, end + 2, replacement.map(_.toString).getOrElse(""))
        }
        last = start
      } while (!isDone)
      sb.toString()
    }
  }

}