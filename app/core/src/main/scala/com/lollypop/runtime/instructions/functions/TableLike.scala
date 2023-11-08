package com.lollypop.runtime.instructions.functions

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_INFRA, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.DatabaseManagementSystem.readPhysicalTable
import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.queryables.RuntimeQueryable
import com.lollypop.runtime.{DatabaseObjectRef, Scope}
import lollypop.io.IOCost

case class TableLike(source: Expression) extends ScalarFunctionCall with RuntimeExpression with RuntimeQueryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scope1, cost1, result1) = source.execute(scope)
    val result2 = result1 match {
      case collection: RowCollection => createTempTable(collection)
      case path: String => createTempTable(readPhysicalTable(DatabaseObjectRef(path).toNS))
      case other => dieIllegalType(other)
    }
    (scope1, cost1, result2)
  }

}

object TableLike extends FunctionCallParserE1(
  name = "tableLike",
  category = CATEGORY_DATAFRAMES_INFRA,
  paradigm = PARADIGM_FUNCTIONAL,
  description =
    """|Creates a new table file, which will be identical to the source table.
       |""".stripMargin,
  example =
    """|val stocksA =
       | |---------------------------------------------------------|
       | | symbol | exchange | lastSale | lastSaleTime             |
       | |---------------------------------------------------------|
       | | NUBD   | NYSE     | 183.8314 | 2023-08-06T03:56:12.932Z |
       | | UAGU   | NASDAQ   | 105.9287 | 2023-08-06T03:56:12.940Z |
       | | XUWH   | NASDAQ   |   58.743 | 2023-08-06T03:56:12.941Z |
       | | EDVC   | NYSE     | 186.1966 | 2023-08-06T03:56:12.943Z |
       | | LFUG   | NYSE     | 128.5487 | 2023-08-06T03:56:12.944Z |
       | |---------------------------------------------------------|
       |val stocksB = tableLike(stocksA)
       |insert into @stocksB from stocksA where lastSale >= 120
       |stocksB
       |""".stripMargin)

