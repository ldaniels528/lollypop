package com.lollypop.runtime.instructions.expressions

import com.lollypop.language._
import com.lollypop.language.models.{Expression, Instruction, Modifiable, Queryable}
import com.lollypop.runtime.instructions.expressions.MacroCall.MacroTemplateTagReplacement
import com.lollypop.runtime.instructions.infrastructure.Macro
import com.lollypop.runtime.instructions.invocables.RuntimeInvokable
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

import scala.annotation.tailrec
import scala.language.postfixOps

/**
 * Macro Call
 * @param _macro the [[Macro]]
 * @param params the input [[Map parameters]]
 */
case class MacroCall(_macro: Macro, params: Map[String, Any]) extends RuntimeInvokable
  with Expression with Modifiable with Queryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val scope1 = params.foldLeft(scope) {
      case (agg, (key, instruction: Instruction)) => agg.withVariable(key, instruction, isReadOnly = true)
      case (agg, (key, value)) => agg.withVariable(key, value, isReadOnly = true)
    }
    val (_, costA, resultA) = _macro.code.execute(scope1)
    (scope, costA, resultA)
  }

  override def toSQL: String = _macro.template.replaceTags(params)

}

object MacroCall {
  private case class Accum(agg: List[String] = Nil)

  /**
   * Template Tag Replacement Extensions
   * @param template the host template (e.g. "calc %e:expr")
   */
  final implicit class MacroTemplateTagReplacement(val template: String) extends AnyVal {

    def replaceTags(params: Map[String, Any]): String = {

      @tailrec
      def processTag(acc: Accum, tag: String, ref: String): Accum = tag match {
        case t if t.startsWith("%") =>
          val value = params.get(ref).collect { case i: Instruction => i.toSQL } || "null"
          acc.copy(agg = value :: acc.agg)
        case t if t.startsWith("?") =>
          processTag(acc, t.substring(1), ref)
        case t if t.startsWith("+?") =>
          processTag(acc, t.substring(2), ref)
        case t =>
          acc.copy(agg = t :: acc.agg)
      }

      def processSegment(acc: Accum, pc: String): Accum = {
        // get the tag and reference
        val (tag, ref) = pc.split(":", 2) match {
          case Array(tag, ref) => (tag, ref)
          case Array(tag) => (tag, "")
        }
        // recursively process the tag
        processTag(acc, tag, ref)
      }

      val acc = template.split(" ").foldLeft[Accum](Accum()) { (acc, pc) => processSegment(acc, pc) }
      acc.agg.reverse.mkString(" ")
    }

  }

}