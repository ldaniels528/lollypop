package com.qwery.runtime.instructions.queryables

import com.qwery.implicits.MagicImplicits
import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language._
import com.qwery.language.models.Instruction.DecompilerAliasHelper
import com.qwery.language.models._
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.{DatabaseObjectRef, SQLRuntimeSupport, Scope}
import qwery.io.IOCost

import scala.collection.mutable
import scala.language.postfixOps

/**
 * Represents a SQL-like select statement
 * @param fields  the given [[Expression columns]]
 * @param from    the given [[Queryable queryable resource]]
 * @param joins   the collection of [[Join join]] clauses
 * @param groupBy the [[FieldRef columns]] by which to group
 * @param having  the aggregation [[Condition condition]]
 * @param orderBy the [[OrderColumn columns]] by which to order
 * @param where   the optional [[Condition where clause]]
 * @param limit   the optional [[Expression maximum number]] of results
 */
case class Select(fields: Seq[Expression] = Seq(AllFields),
                  from: Option[Queryable] = None,
                  joins: Seq[Join] = Nil,
                  groupBy: Seq[FieldRef] = Nil,
                  having: Option[Condition] = None,
                  orderBy: Seq[OrderColumn] = Nil,
                  where: Option[Condition] = None,
                  limit: Option[Expression] = None)
  extends RuntimeQueryable with SQLRuntimeSupport {

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    search(scope, this) ~> { case (c, r) => (scope, c, r) }
  }

  override def toSQL: String = {

    def dereference(q: Queryable): String = q match {
      case r: DatabaseObjectRef => r.toSQL
      case r => s"(${r.toSQL})"
    }

    val sb = new mutable.StringBuilder(s"select ${fields.map(f => f.toSQL.withAlias(f.alias)).mkString(", ")}")
    from.foreach(queryable => sb.append(s" from ${dereference(queryable)}"))
    if (joins.nonEmpty) sb.append(" ")
    sb.append(joins.map(_.toSQL).mkString(" "))
    where.foreach(condition => sb.append(s" where ${condition.toSQL}"))
    if (groupBy.nonEmpty) sb.append(s" group by ${groupBy.map(_.toSQL).mkString(", ")}")
    having.foreach(condition => sb.append(s" having ${condition.toSQL}"))
    if (orderBy.nonEmpty) sb.append(s" order by ${orderBy.map(_.toSQL).mkString(", ")}")
    limit.foreach(n => sb.append(s" limit ${n.toSQL}"))
    sb.toString()
  }

}

object Select extends QueryableParser {
  val templateCard = "select %E:fields ?from +?%q:source"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "select",
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns row(s) of data based on the expression and options",
    example = "select symbol: 'GMTQ', exchange: 'OTCBB', lastSale: 0.1111, lastSaleTime: DateTime()"
  ))

  override def parseQueryable(stream: TokenStream)(implicit compiler: SQLCompiler): Queryable = {
    val params = SQLTemplateParams(stream, templateCard)
    Queryable(stream, Select(
      fields = params.expressionLists("fields"),
      from = params.instructions.get("source").map {
        case q: Queryable => q
        case z => From(z)
      }
    ))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "select"

}