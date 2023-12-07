package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_IO, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models._
import com.lollypop.runtime._
import lollypop.io.IOCost

import scala.language.postfixOps

/**
 * Removes rows from a table
 * @param ref       the [[DatabaseObjectRef]]
 * @param condition the optional [[Condition condition]]
 * @param limit     the [[Expression limit]]
 * @example {{{
 *  delete from stocks where symbol is 'SHMN'
 * }}}
 * @example {{{
 *  delete from stocks#transactions
 *  where symbol is 'SHMN'
 *  and wherein transactions (price is 0.001)
 * }}}
 */
case class Delete(ref: DatabaseObjectRef, condition: Option[Condition], limit: Option[Expression])
  extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val cost = ref match {
      // is it an inner table reference? (e.g. 'stocks#transactions')
      case st: DatabaseObjectRef if st.isSubTable =>
        st.inside { case (outerTable, innerTableColumn) =>
          outerTable.deleteInside(innerTableColumn, condition, limit)
        }
      // must be a standard table reference (e.g. 'stocks')
      case _ => scope.getRowCollection(ref).deleteWhere(condition, limit)
    }
    (scope, cost, cost)
  }

  override def toSQL: String = {
    (List("delete from", ref.toSQL) ::: condition.toList.flatMap(c => List("where", c.toSQL)) :::
      limit.toList.flatMap(n => List("limit", n.toSQL))).mkString(" ")
  }

}

object Delete extends ModifiableParser {
  val template: String = "delete from %L:name ?where +?%c:condition ?limit +?%e:limit"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "delete",
    category = CATEGORY_DATAFRAMES_IO,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Deletes rows matching an expression from a table",
    example =
      """|val stocks =
         ||------------------------------|
         || symbol | exchange | lastSale |
         ||------------------------------|
         || OWWO   | NYSE     | 483.0286 |
         || SJJR   | OTCBB    |  56.7381 |
         || EGXY   | OTCBB    | 309.8648 |
         || NXSQ   | OTCBB    | 254.2278 |
         || LQRQ   | AMEX     |    88.42 |
         ||------------------------------|
         |delete from @stocks where symbol is "EGXY"
         |stocks
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Delete] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      Some(Delete(
        ref = params.locations("name"),
        condition = params.conditions.get("condition"),
        limit = params.expressions.get("limit")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "delete from"

}
