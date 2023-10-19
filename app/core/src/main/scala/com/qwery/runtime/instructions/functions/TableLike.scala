package com.qwery.runtime.instructions.functions

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.Expression
import com.qwery.runtime.DatabaseManagementSystem.readPhysicalTable
import com.qwery.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.devices.RowCollectionZoo._
import com.qwery.runtime.instructions.expressions.RuntimeExpression
import com.qwery.runtime.instructions.queryables.RuntimeQueryable
import com.qwery.runtime.{DatabaseObjectRef, QweryVM, Scope}
import qwery.io.IOCost

case class TableLike(source: Expression) extends ScalarFunctionCall with RuntimeExpression with RuntimeQueryable {

  override def evaluate()(implicit scope: Scope): RowCollection = {
    val (_, _, result1) = QweryVM.execute(scope, source)
    result1 match {
      case collection: RowCollection => createTempTable(collection)
      case path: String => createTempTable(readPhysicalTable(DatabaseObjectRef(path).toNS))
      case other => dieIllegalType(other)
    }
  }

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = (scope, IOCost.empty, evaluate())
}

object TableLike extends FunctionCallParserE1(
  name = "tableLike",
  category = CATEGORY_DATAFRAME,
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
       |insert into @@stocksB from stocksA where lastSale >= 120
       |stocksB
       |""".stripMargin)

