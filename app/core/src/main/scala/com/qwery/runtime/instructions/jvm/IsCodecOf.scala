package com.qwery.runtime.instructions.jvm

import com.qwery.language.ColumnTypeParser.nextColumnType
import com.qwery.language.HelpDoc.{CATEGORY_BRANCHING_OPS, PARADIGM_IMPERATIVE}
import com.qwery.language.models.{ColumnType, Condition, Expression}
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.datatypes.{DataType, Inferences}
import com.qwery.runtime.instructions.conditions.RuntimeCondition
import com.qwery.runtime.instructions.jvm.IsCodecOf.__name
import com.qwery.runtime.{QweryVM, Scope}

/**
 * isCodecOf instruction
 * @param expr   the [[Expression expression]] to evaluate
 * @param `type` the [[ColumnType type]] to compare
 * @example {{{ getNextItem() isCodecOf Int }}}
 */
case class IsCodecOf(expr: Expression, `type`: ColumnType) extends RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    val v = QweryVM.execute(scope, expr)._3
    Inferences.fromValue(v).name == DataType.load(`type`).name
  }

  override def toSQL: String = List(expr.wrapSQL, __name, `type`.wrapSQL).mkString(" ")

}

object IsCodecOf extends ExpressionToConditionPostParser {
  private val __name = "isCodecOf"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Condition] = {
    if (ts.nextIf(__name)) Option(IsCodecOf(host, nextColumnType(ts))) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = __name,
    category = CATEGORY_BRANCHING_OPS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = s"`expression` ${__name} `CODEC`",
    description = "determines whether the `expression` is compatible with the `CODEC`",
    example = "(new `java.util.Date`()) isCodecOf DateTime"
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}