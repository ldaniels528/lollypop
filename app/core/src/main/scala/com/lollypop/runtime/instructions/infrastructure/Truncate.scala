package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_INFRA, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.runtime.{DatabaseObjectRef, Scope}
import lollypop.io.IOCost

/**
 * Represents a SQL truncate statement
 * @param ref the [[DatabaseObjectRef table]] to update
 */
case class Truncate(ref: DatabaseObjectRef) extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val cost = scope.getRowCollection(ref).setLength(newSize = 0)
    (scope, cost, cost)
  }

  override def toSQL: String = s"truncate ${ref.toSQL}"

}

object Truncate extends ModifiableParser {
  val templateCard: String = "truncate ?table %L:target"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "truncate",
    category = CATEGORY_DATAFRAMES_INFRA,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Removes all of the data from a table",
    example =
      """|val stocks =
         |  |---------------------------------------------------------|
         |  | symbol | exchange | lastSale | lastSaleTime             |
         |  |---------------------------------------------------------|
         |  | CJHK   | OTCBB    |  36.4423 | 2023-08-03T00:09:42.263Z |
         |  | OZIS   | NYSE     |  97.3854 | 2023-08-03T00:09:42.279Z |
         |  | DKRA   | NASDAQ   | 127.5813 | 2023-08-03T00:09:42.280Z |
         |  | IWEC   | AMEX     | 132.1874 | 2023-08-03T00:09:42.282Z |
         |  | JIRD   | OTCBB    |  22.0003 | 2023-08-03T00:09:42.283Z |
         |  |---------------------------------------------------------|
         |truncate @@stocks
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Truncate] = {
    if (understands(ts)) {
      Some(Truncate(ref = SQLTemplateParams(ts, "truncate ?table %L:target").locations("target")))
    } else None
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "truncate"

}
