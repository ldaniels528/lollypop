package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.models.{@@, Expression, Queryable}
import com.lollypop.language.{HelpDoc, QueryableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.datatypes.{Inferences, TableType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.devices.{RowCollection, TableColumn}
import com.lollypop.runtime.instructions.expressions.TableExpression
import com.lollypop.runtime.instructions.queryables.AssumeQueryable.EnrichedAssumeQueryable
import com.lollypop.runtime.{LollypopVM, Scope}

/**
 * Represents rows of values (e.g. "values (2, 5, 7, 11), (13, 17, 19, 23)")
 * @param values a list of a list of [[Expression values]]
 */
case class RowsOfValues(values: List[List[Expression]]) extends Queryable with TableExpression with TableRendering {

  override def isChainable: Boolean = false

  override def returnType: TableType = toTableType

  override def toSQL: String = s"values ${values.map(lst => s"(${lst.map(_.toSQL).mkString(",")})").mkString(", ")}"

  override def toTable(implicit scope: Scope): RowCollection = {
    val columns = returnType.columns
    val rc = createQueryResultTable(columns)
    for {
      row <- values
      mapping = Map((columns.map(_.name) zip row).map { case (name, expr) =>
        name -> LollypopVM.execute(scope, expr)._3
      }: _*)
    } rc.insert(mapping.toRow(rc))
    rc
  }

  override def toTableType: TableType = {
    val columns = for {
      row <- values.headOption.toList
      (column, n) <- row.zipWithIndex
    } yield TableColumn(name = String.valueOf(('A' + n).toChar), `type` = Inferences.inferType(column))
    TableType(columns)
  }


}

object RowsOfValues extends QueryableParser {

  override def help: List[HelpDoc] = Nil

  override def parseQueryable(stream: TokenStream)(implicit compiler: SQLCompiler): Option[Queryable] = {
    //if (understands(stream)) {
      val source: Queryable = stream match {
        // values clause?
        case ts if ts nextIf "values" =>
          ts match {
            case ts1 if ts1 nextIf "@" => @@(ts1.next().valueAsString)
            case ts1 if ts1 nextIf "$" => ts1.dieScalarVariableIncompatibleWithRowSets()
            case ts1 =>
              var values: List[List[Expression]] = Nil
              do values = SQLTemplateParams(ts1, "( %E:values )").expressionLists("values") :: values while (ts1 nextIf ",")
              RowsOfValues(values.reverse)
          }
        // variable?
        case ts if ts nextIf "@" => @@(ts.next().valueAsString)
        case ts if ts nextIf "$" => ts.dieScalarVariableIncompatibleWithRowSets()
        // any supported query ...
        case ts => compiler.nextQueryOrVariableWithAlias(ts).asQueryable
      }
      Some(source)
    //} else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "values"

}