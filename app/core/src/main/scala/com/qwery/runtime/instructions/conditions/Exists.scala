package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Instruction
import com.qwery.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.{QweryVM, Scope}

/**
 * SQL: exists( select column_name from table_name where condition )
 * @param instruction the given [[Instruction instruction]]
 */
case class Exists(instruction: Instruction) extends RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    QweryVM.execute(scope, instruction)._3 match {
      case d: RowCollection => d.getLength > 0
      case _ => false
    }
  }

  override def toSQL: String = s"exists ${instruction.toSQL}"

}

object Exists extends ExpressionParser {
  private val __name = "exists"

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Exists] = {
    if (ts.nextIf(__name)) {
      Option(Exists(compiler.nextQueryOrVariableWithAlias(ts)))
    } else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = __name,
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"${__name}(`query`)",
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
         |@@stocks where lastSale > 5 and exists(select symbol from @@stocks where exchange is 'OTCBB')
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}