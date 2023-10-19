package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language.models.{Condition, Expression}
import com.qwery.language.{HelpDoc, ModifiableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.devices.RowCollectionZoo.RichDatabaseObjectRef
import com.qwery.runtime.{DatabaseObjectRef, Scope}
import qwery.io.IOCost

/**
 * Restores previously deleted rows
 * @param ref       the [[DatabaseObjectRef]]
 * @param condition the optional [[Condition condition]]
 * @param limit     the [[Expression limit]]
 */
case class UnDelete(ref: DatabaseObjectRef,
                    condition: Option[Condition],
                    limit: Option[Expression])
  extends RuntimeModifiable {

  override def invoke()(implicit scope: Scope): (Scope, IOCost) = {
    val cost = ref match {
      // is it an inner table reference? (e.g. 'stocks#transactions')
      case st: DatabaseObjectRef if st.isSubTable =>
        st.inside { case (outerTable, innerTableColumn) =>
          outerTable.undeleteInside(innerTableColumn, condition, limit)
        }
      // must be a standard table reference (e.g. 'stocks')
      case _ => scope.getRowCollection(ref).undeleteWhere(condition, limit)
    }
    scope -> cost
  }

  override def toSQL: String = {
    (List("undelete from", ref.toSQL) ::: condition.toList.flatMap(c => List("where", c.toSQL)) :::
      limit.toList.flatMap(n => List("limit", n.toSQL))).mkString(" ")
  }

}

object UnDelete extends ModifiableParser {
  val template: String = "undelete from %L:name ?where +?%c:condition ?limit +?%e:limit"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "undelete",
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Restores rows matching an expression from a table",
    example =
      """|val stocks =
         | |---------------------------------------------------------|
         | | symbol | exchange | lastSale | lastSaleTime             |
         | |---------------------------------------------------------|
         | | CMHA   | NASDAQ   | 121.4325 | 2023-08-05T22:45:29.370Z |
         | | JPJI   | NYSE     | 185.8192 | 2023-08-05T22:45:29.371Z |
         | | QCYA   | AMEX     | 152.0165 | 2023-08-05T22:45:29.372Z |
         | | TGRV   | NYSE     |   80.225 | 2023-08-05T22:45:29.373Z |
         | | XHMQ   | NASDAQ   |   98.445 | 2023-08-05T22:45:29.374Z |
         | |---------------------------------------------------------|
         |delete from @@stocks where symbol is "CMHA"
         |undelete from @@stocks where symbol is "CMHA"
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): UnDelete = {
    val params = SQLTemplateParams(ts, template)
    UnDelete(ref = params.locations("name"),
      condition = params.conditions.get("condition"),
      limit = params.expressions.get("limit"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "undelete"

}
