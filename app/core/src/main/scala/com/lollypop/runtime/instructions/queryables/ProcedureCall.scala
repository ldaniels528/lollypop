package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_IMPERATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{Expression, FunctionCall, Queryable}
import com.lollypop.runtime.DatabaseManagementSystem.readProcedure
import com.lollypop.runtime._
import com.lollypop.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.lollypop.runtime.devices.{RecordStructure, RowCollection, TableColumn}
import com.lollypop.runtime.instructions.invocables.RuntimeInvokable
import lollypop.io.IOCost

/**
 * Invokes a procedure by name
 * @param ref  the [[DatabaseObjectRef procedure name]]
 * @param args the collection of [[Expression arguments]] to be passed to the procedure upon invocation
 * @example {{{ call getEligibleFeeds("/home/ubuntu/feeds/") }}}
 */
case class ProcedureCall(ref: DatabaseObjectRef, args: List[Expression]) extends RuntimeInvokable
  with Queryable with FunctionCall {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    // get the procedure and in and OUT parameters
    val procedure = readProcedure(ref.toNS)
    val (paramsOUT, paramsIN) = procedure.params.partition(_.isOutput)
    assert(paramsIN.length == args.length, this.dieArgumentMismatch(paramsIN.length, paramsIN.length, paramsIN.length))

    // execute the procedure
    val (scope1, cost1, result1) = procedure.code.execute(scope.withParameters(paramsIN, args))

    // interpret the result
    val result2 = Option(result1).collect { case d: RowCollection => d } || this.dieIllegalType(result1)
    if (paramsOUT.nonEmpty) {
      // if there are any OUT parameters then expect 1 row exactly
      if (result2.isEmpty) this.die("No result returned")
      else if (result2.getLength > 1) this.die("Multiple rows returned")
      else {
        // create a Map containing the output properties
        val values = result2(0).toMap
        val result3 = paramsOUT.foldLeft[Map[String, Any]](Map.empty) { case (agg, param) =>
          agg + (param.name -> (values.get(param.name) ?? param.defaultValue.map(_.execute(scope)._3)).orNull)
        }
        val result4 = result3.toRow(0L)(new RecordStructure {
          override val columns: Seq[TableColumn] = paramsOUT.map(_.toColumn).map(_.toTableColumn)
        }).toRowCollection
        (scope1, cost1, result4)
      }
    } else (scope1, cost1, result2)
  }

  override def toSQL: String = s"call ${ref.toSQL}${args.map(_.toSQL).mkString("(", ", ", ")")}"

}

object ProcedureCall extends QueryableParser {

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "call",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "Executes a stored procedure; returns a row set",
    example =
      """|namespace "test.examples"
         |stockQuotes =
         | |---------------------------------------------------------|
         | | symbol | exchange | lastSale | lastSaleTime             |
         | |---------------------------------------------------------|
         | | DBGK   | AMEX     |  46.2471 | 2023-08-06T04:50:07.478Z |
         | | GROT   | NASDAQ   |  44.3673 | 2023-08-06T04:50:07.480Z |
         | | SCOF   | NASDAQ   |  60.8058 | 2023-08-06T04:50:07.482Z |
         | | CYCR   | NASDAQ   |  83.9982 | 2023-08-06T04:50:07.483Z |
         | | IIDA   | NASDAQ   | 126.3182 | 2023-08-06T04:50:07.484Z |
         | |---------------------------------------------------------|
         |drop if exists getStockQuote
         |create procedure getStockQuote(theExchange: String,
         |                               --> exchange: String,
         |                               --> total: Double,
         |                               --> maxPrice: Double,
         |                               --> minPrice: Double) as
         |  select exchange, total: count(*), maxPrice: max(lastSale), minPrice: min(lastSale)
         |  from @stockQuotes
         |  where exchange is theExchange
         |  group by exchange
         |
         |call getStockQuote("NASDAQ")
         |""".stripMargin
  ))

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[ProcedureCall] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      Some(ProcedureCall(ref = params.locations("name"), args = params.expressionLists("args")))
    } else None
  }

  val templateCard: String = "call %L:name %A:args"

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "call"

}
