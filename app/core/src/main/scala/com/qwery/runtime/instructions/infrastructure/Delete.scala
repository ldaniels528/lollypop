package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language._
import com.qwery.language.models._
import com.qwery.runtime.devices.RowCollectionZoo.RichDatabaseObjectRef
import com.qwery.runtime.{DatabaseObjectRef, Scope}
import qwery.io.IOCost

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

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val cost = ref match {
      // is it an inner table reference? (e.g. 'stocks#transactions')
      case st: DatabaseObjectRef if st.isSubTable =>
        st.inside { case (outerTable, innerTableColumn) =>
          outerTable.deleteInside(innerTableColumn, condition, limit)
        }
      // must be a standard table reference (e.g. 'stocks')
      case _ => scope.getRowCollection(ref).deleteWhere(condition, limit)
    }
    (scope, cost, true)
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
    category = CATEGORY_DATAFRAME,
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
         |delete from @@stocks where symbol is "EGXY"
         |stocks
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Delete = {
    val params = SQLTemplateParams(ts, template)
    Delete(ref = params.locations("name"), condition = params.conditions.get("condition"), limit = params.expressions.get("limit"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "delete from"

}
