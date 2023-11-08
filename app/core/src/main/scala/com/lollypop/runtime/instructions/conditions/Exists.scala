package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Instruction
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.devices.RowCollection
import lollypop.io.IOCost

/**
 * SQL: exists( select column_name from table_name where condition )
 * @param instruction the given [[Instruction instruction]]
 */
case class Exists(instruction: Instruction) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val (s, c, r) = instruction.execute(scope)
    (s, c, r match {
      case d: RowCollection => d.getLength > 0
      case _ => false
    })
  }

  override def toSQL: String = s"exists ${instruction.toSQL}"

}

object Exists extends ExpressionParser {
  private val keyword = "exists"

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Exists] = {
    if (ts.nextIf(keyword)) {
      Option(Exists(compiler.nextQueryOrVariableWithAlias(ts)))
    } else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"$keyword %q:query",
    description = "determines whether at least one row is found within the query",
    example =
      """|val stocks = (
         ||---------------------------------------------------------|
         || exchange | symbol | lastSale | lastSaleTime             |
         ||---------------------------------------------------------|
         || OTCBB    | BKULD  |   0.8745 | 2023-09-26T21:30:24.127Z |
         || OTCBB    | SAFXV  |   0.8741 | 2023-09-26T21:30:13.488Z |
         || NASDAQ   | ECN    |  36.9565 | 2023-09-26T21:30:05.816Z |
         || AMEX     | HRB    | 164.4908 | 2023-09-26T21:30:41.457Z |
         || NASDAQ   | CFF    | 107.4943 | 2023-09-26T21:30:06.283Z |
         ||---------------------------------------------------------|
         |)
         |@stocks where lastSale > 5 and exists(select symbol from @stocks where exchange is 'OTCBB')
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}