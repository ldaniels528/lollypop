package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.ColumnTypeParser.nextColumnType
import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.{ColumnType, Condition, Expression}
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.datatypes.{DataType, Inferences}
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.instructions.jvm.IsCodecOf.__name
import com.lollypop.runtime.{LollypopVM, Scope}

/**
 * isCodecOf instruction
 * @param expr   the [[Expression expression]] to evaluate
 * @param `type` the [[ColumnType type]] to compare
 * @example {{{ getNextItem() isCodecOf Int }}}
 */
case class IsCodecOf(expr: Expression, `type`: ColumnType) extends RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    val v = LollypopVM.execute(scope, expr)._3
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
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"`expression` ${__name} `CODEC`",
    description = "determines whether the `expression` is compatible with the `CODEC`",
    example = "(new `java.util.Date`()) isCodecOf DateTime"
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}