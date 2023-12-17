package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.ColumnTypeParser.nextColumnType
import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.{ColumnType, Condition, Expression}
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.datatypes.{DataType, Inferences}
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.instructions.jvm.IsCodecOf.keyword
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * isCodecOf instruction
 * @param expr   the [[Expression expression]] to evaluate
 * @param `type` the [[ColumnType type]] to compare
 * @example {{{ getNextItem() isCodecOf Int }}}
 */
case class IsCodecOf(expr: Expression, `type`: ColumnType) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val v = expr.execute(scope)._3
    val result = Inferences.fromValue(v).name == DataType.load(`type`).name
    (scope, IOCost.empty, result)
  }

  override def toSQL: String = List(expr.wrapSQL, keyword, `type`.wrapSQL).mkString(" ")

}

object IsCodecOf extends ExpressionToConditionPostParser {
  private val keyword = "isCodecOf"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Condition] = {
    if (ts.nextIf(keyword)) Option(IsCodecOf(host, nextColumnType(ts))) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"`expression` $keyword `CODEC`",
    description = "determines whether the `expression` is compatible with the `CODEC`",
    example = "(new `java.util.Date`()) isCodecOf DateTime"
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}