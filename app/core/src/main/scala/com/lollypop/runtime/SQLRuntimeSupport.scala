package com.lollypop.runtime

import com.lollypop.die
import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.dieUnsupportedEntity
import com.lollypop.language.models.Expression.implicits.RichAliasable
import com.lollypop.language.models.Inequality.toInequalities
import com.lollypop.language.models.{AllFields, Condition, Expression, FieldRef, Function, Inequality, Literal, OrderColumn, Queryable}
import com.lollypop.runtime.LollypopVM.sort
import com.lollypop.runtime.SQLRuntimeSupport.ColumnValidation
import com.lollypop.runtime.datatypes.Inferences.{InstructionTyping, resolveType}
import com.lollypop.runtime.datatypes.{DataType, Inferences, TableType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo.{createMemoryTable, createQueryResultTable}
import com.lollypop.runtime.devices._
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.instructions.conditions.RuntimeCondition.isTrue
import com.lollypop.runtime.instructions.conditions.RuntimeInequality.OptionComparator
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.instructions.expressions._
import com.lollypop.runtime.instructions.expressions.aggregation._
import com.lollypop.runtime.instructions.functions.InternalFunctionCall
import com.lollypop.runtime.instructions.functions.ScalarFunctionCall.ArgumentExtraction
import com.lollypop.runtime.instructions.queryables._
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.ResourceHelper._
import lollypop.io.IOCost

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.language.postfixOps

/**
 * SQL Runtime Support
 */
object SQLRuntimeSupport extends SQLRuntimeSupport {

  final implicit class ColumnValidation[T <: Expression](val fields: Seq[T]) extends AnyVal {
    def validate: Seq[T] = {
      fields foreach {
        case fx: Function if fx.alias.isEmpty => die("Functions must have an alias")
        case _ =>
      }
      fields
    }
  }

}

/**
 * SQL Runtime Support
 */
trait SQLRuntimeSupport {

  def search(scope: Scope, select: Select): (IOCost, RowCollection) = {
    select.from match {
      case Some(_) if select.joins.nonEmpty => selectAndInnerJoin(scope, select)
      case Some(_) if select.groupBy.nonEmpty || isAggregation(select.fields) => selectAndAggregate(scope, select)
      case Some(_) => selectAndTransform(select)(scope)
      case None => selectAndProduce(select)(scope)
    }
  }

  /**
   * Indicates whether the projection contains at least one aggregate function
   * @param projection the collection of projection [[Expression expressions]]
   * @return true, if the projection contains at least one aggregate function
   */
  private def isAggregation(projection: Seq[Expression]): Boolean = projection.exists(_.isAggregation)

  /**
   * Evaluates an aggregation query
   * @param scope  the [[Scope scope]]
   * @param select the [[Select select model]]
   * @return a new [[RowCollection collection]] containing the results
   */
  private def selectAndAggregate(scope: Scope, select: Select): (IOCost, RowCollection) = {
    val (scope0, cost0, source) = select.from.map(LollypopVM.search(scope, _)) || select.dieMissingSource()
    val selectedColumns: Seq[TableColumn] = getTransformationColumns(select.fields, source.columns)
    val selectedFields: Seq[Expression] = expandSelection(select.fields, selectedColumns).validate
    val out = createQueryResultTable(selectedColumns)

    def isHavingSatisfied(state: Map[String, Any]): Boolean = {
      select.having.isEmpty || Scope(scope0, state) ~> { myScope => select.having.exists(c => isTrue(c)(myScope)) }
    }

    def toRowMapping(aggExpressions: Seq[AggregateProjection], aggregators: Seq[Aggregator]): Map[String, Any] = {
      Map(aggExpressions zip aggregators flatMap { case (expr, agg) => agg.collect(scope0).map(value => expr.name -> value) }: _*)
    }

    // is it a summarization?
    if (select.groupBy.isEmpty) {
      val aggExpressions = getAggregateProjection(selectedFields)(scope0)
      val aggregators = aggExpressions.map(_.aggregate)
      source.iterateWhere(select.where, select.limit)(_.isActive) { (scope1, _) =>
        aggregators.foreach(_.update(scope1))
      }(scope0)
      val mapping = toRowMapping(aggExpressions, aggregators)
      out.insert(mapping.toRow(out))
    }

    // otherwise, it's an aggregation
    else {
      // get group by and intermediate columns
      val groupByColumnNames: Seq[String] = select.groupBy.map(_.name)
      val intermediateColumns: Seq[TableColumn] = getReferencedColumns(selectedFields, source.columns)

      // partition the rows into temporary tables per grouped key
      val separator = String.valueOf(Array[Char](2, 5, 7))
      val partitions = mutable.Map[String, RowCollection]()
      source.iterateWhere(select.where, select.limit)(_.isActive) { (_, row) =>
        val key = groupByColumnNames.flatMap(row.getField).map(_.value.getOrElse("")).mkString(separator)
        val tempFile = partitions.getOrElseUpdate(key, createQueryResultTable(intermediateColumns))
        val mapping = Map(intermediateColumns.map(ic => ic.name -> row.getField(ic.name).flatMap(_.value)): _*)
        tempFile.insert(mapping.toRow(tempFile))
      }(scope0)

      // aggregate the results
      partitions.values foreach { implicit partition =>
        val aggExpressions: Seq[AggregateProjection] = getAggregateProjection(selectedFields)(scope0)
        val aggregators = aggExpressions.map(_.aggregate)
        partition.use(_.iterateWhere()(_.isActive) { (rowScope, _) => aggregators.foreach(_.update(rowScope)) }(scope0))
        val mapping = toRowMapping(aggExpressions, aggregators)
        if (isHavingSatisfied(mapping)) out.insert(mapping.toRow(out))
      }
    }
    sort(out, select.orderBy) ~> { case (cost1, rc) => (cost0 ++ cost1, rc) }
  }

  /**
   * Evaluates a query without a source
   * @param select the [[Select select model]]
   * @param scope  the implicit [[Scope scope]]
   * @return a new [[RowCollection collection]] containing the result
   */
  private def selectAndProduce(select: Select)(implicit scope: Scope): (IOCost, RowCollection) = {
    val results = select.fields.map {
      case AllFields => die("All fields (*) cannot be expanded without a query source")
      case expr =>
        val value = LollypopVM.execute(scope, expr)._3
        val column = TableColumn(name = expr.getNameOrDie, `type` = Inferences.fromValue(value))
        (column, value)
    }

    // write a single record
    results.flatMap(v => Option(v._2).toSeq).headOption match {
      case Some(device: RowCollection) if results.size == 1 => IOCost() -> device
      case _ =>
        implicit val out: RowCollection = createQueryResultTable(columns = results.map(_._1), fixedRowCount = 1)
        val cost = out.insert(Map(results.map { case (column, value) => column.name -> value }: _*).toRow(out))
        cost -> out
    }
  }

  /**
   * Evaluates a transformation query
   * @param select the [[Select select model]]
   * @param scope  the implicit [[Scope scope]]
   * @return a new [[RowCollection collection]] containing the results
   */
  private def selectAndTransform(select: Select)(implicit scope: Scope): (IOCost, RowCollection) = {
    val (_, cost0, source) = select.from.map(LollypopVM.search(scope, _)) || select.dieMissingSource()
    val selectedColumns = getTransformationColumns(select.fields, source.columns)
    val selectedFields = expandSelection(select.fields, selectedColumns)
    val isExpansion = selectedFields.exists(_.isInstanceOf[UnNest])

    // generate the results
    val out = createMemoryTable(selectedColumns)
    val cost1 = source.iterateWhere(select.where, select.limit)(_.isActive) { (rowScope, incomingRow) =>
      val rawValues = selectedFields.map(LollypopVM.execute(rowScope, _)._3)
      processOutgoingRow(selectedColumns, incomingRow, rawValues.map(Option.apply), isExpansion)(out)
    }

    // order the results?
    sort(out, select.orderBy) ~> { case (cost2, rc) => (cost0 ++ cost1 ++ cost2, rc) }
  }

  private def processOutgoingRow(selectedColumns: Seq[TableColumn],
                                 incomingRow: Row,
                                 rawValues: Seq[Option[Any]],
                                 isExpansion: Boolean)(out: RowCollection): Unit = {
    // do we have a row expansion event?
    if (isExpansion && (rawValues.exists(_.exists(_.isInstanceOf[RowCollection])) || rawValues.exists(_.exists(_.isInstanceOf[Iterable[Row]])))) {
      for {
        value_? <- rawValues
        rows <- value_?.collect {
          case i: Iterable[Row] => i.toList
          case r: RowCollection => r.toList
        }
      } {
        val targetRow = Map(selectedColumns.map(_.name) zip rawValues.map(_.orNull): _*)
        rows.foreach { row =>
          val explodedRow = (targetRow ++ row.toMap ++
            Map(SRC_ROWID_NAME -> incomingRow.id)).filterNot(_._1 == ROWID_NAME)
          out.insert(explodedRow.toRow(out))
        }
      }
    } else {
      // just write a single row
      val targetRow = (Map(selectedColumns.map(_.name) zip rawValues.map(_.orNull): _*) ++
        Map(SRC_ROWID_NAME -> incomingRow.id)).filterNot(_._1 == ROWID_NAME)
      out.insert(targetRow.toRow(out))
    }
  }

  /**
   * Evaluates a multi-table transformation query
   * @param scope0 the [[Scope scope]]
   * @param select the [[Select select model]]
   * @return a new [[RowCollection collection]] containing the results
   */
  private def selectAndInnerJoin(scope0: Scope, select: Select): (IOCost, RowCollection) = {
    // determine the collection of secondary sources
    val secondaries = select.joins.toList.map { j =>
      val columnNames = getReferencedJoinColumns(j.condition, j.source.getNameOrDie)
      val (cost, device) = createJoinDevice(j.source, columnNames: _*)(scope0)
      JoinSource(tableAlias = j.source.getNameOrDie, device, columnNames, condition = Some(j.condition))
    }

    // determine the primary source
    val primary = select.from.map { f =>
      val columnNames = secondaries.flatMap(j => j.condition.toList.flatMap(c => getReferencedJoinColumns(c, f.getNameOrDie))).distinct
      val (cost, device) = createJoinDevice(f, columnNames: _*)(scope0)
      JoinSource(tableAlias = f.getNameOrDie, device, columnNames, condition = None)
    } || die("Primary source was not specified")

    // register the table aliases
    val devices = primary :: secondaries
    val scope1 = devices.foldLeft[Scope](scope0) {
      case (agg, JoinSource(tableAlias, device, _, _)) => agg.withDataSource(tableAlias, device)
    }

    val secondariesM: Map[String, JoinSource] = Map(secondaries.map(joinSource => joinSource.tableAlias -> joinSource): _*)
    val allColumns = devices.flatMap(_.device.columns).distinct
    val lookupType: String => DataType = name => resolveType(allColumns.collect { case c if c.name == name => c.`type` }: _*)

    // create a grid containing the "evaluated fields" or node (e.g., cells of a spreadsheet)
    case class Node(name: String, expression: Expression, `type`: DataType, source: Option[String] = None)
    val cells = select.fields.toList flatMap {
      case AllFields => allColumns.map { c => Node(name = c.name, expression = FieldRef(c.name), `type` = c.`type`) }
      case b: BasicFieldRef => Node(name = b.name, expression = b, `type` = lookupType(b.name)) :: Nil
      case j@Infix(_, AllFields) =>
        val columns = secondariesM.get(j.getNameOrDie).toList.flatMap(_.device.columns)
        columns.map(c => Node(name = c.name, expression = FieldRef(c.name), `type` = c.`type`))
      case Infix(t, f) => (t.getNameOrDie, f.getNameOrDie) ~> { case (tableAlias, name) =>
        Node(name = name, expression = JoinFieldRef(tableAlias, name), `type` = lookupType(name), source = Some(tableAlias)) :: Nil
      }
      case expr => Node(name = expr.getNameOrDie, expression = expr, `type` = expr.returnType) :: Nil
    }

    // determine the selected (fields, columns) and nesting disposition
    val selectedColumns = cells.map(c => TableColumn(name = c.expression.getNameOrDie, `type` = c.`type`))
    val selectedFields = cells.map(_.expression)

    val out = createQueryResultTable(selectedColumns)
    joinRows(primary, secondaries, select.limit) { case (scope2, row) =>
      assert(scope2.getCurrentRow contains row)
      val (_, c2, values) = LollypopVM.transform(scope2, selectedFields)
      val targetRow = Map(selectedColumns.map(_.name) zip values: _*) ++ Map(SRC_ROWID_NAME -> row.id)
      out.insert(targetRow.toRow(out))
    }(scope1)

    // order the results?
    sort(out, select.orderBy)
  }

  private def joinRows(primarySrc: JoinSource, secondaries: List[JoinSource], limit: Option[Expression])(f: (Scope, Row) => Unit)(implicit scope0: Scope): Long = {
    val primary = primarySrc.device

    def makeRow(rowID: ROWID, mapping: Map[String, Any]): Row = {
      Row(id = rowID, metadata = RowMetadata(), columns = primary.columns, fields = mapping.toList.map { case (name, value) =>
        Field(name = name, metadata = FieldMetadata(), value = Option(value))
      })
    }

    // perform search
    val counter = new AtomicLong(0)
    var (matches: ROWID, rowID: ROWID) = (0L, 0L)
    val _limit = limit.flatMap(_.asInt32)
    while (primary.hasNext && (_limit.isEmpty || _limit.exists(counter.addAndGet(1) <= _))) {
      // read rows from each of the devices
      var row0: Option[Row] = Some(primary.next())
      val rowN = secondaries.map { case JoinSource(_, secondary, _, condition_?) =>
        var row_? : Option[Row] = None
        do {
          row_? = if (secondary.hasNext) Some(secondary.next()) else None
          val (a, b) = (row0.flatMap(_.getField("symbol")).flatMap(_.value), row_?.flatMap(_.getField("symbol")).flatMap(_.value))
          (a, b) match {
            case (a, b) if a > b => row_? = if (secondary.hasNext) Some(secondary.next()) else None
            case (a, b) if a < b => row0 = if (primary.hasNext) Some(primary.next()) else None
            case _ =>
          }
        } while (secondary.hasNext && condition_?.exists(!RuntimeCondition.isTrue(_)))
        row_?
      }

      // build the row and invoke the callback
      if (rowN.forall(_.nonEmpty)) {
        val row = makeRow(rowID, Map(rowN.flatten.flatMap(_.toMap): _*) ++ row0.toList.flatMap(_.toMap))
        f(scope0.withCurrentRow(Some(row)), row)
        matches += 1
      }
      rowID += 1
    }
    matches
  }

  private def expandSelection(expressions: Seq[Expression], selectedColumns: Seq[TableColumn]): Seq[Expression] = {
    expressions flatMap {
      case AllFields => selectedColumns.map(_.name).map(FieldRef.apply)
      case expression => Seq(expression)
    }
  }

  private def getAggregateProjection(expressions: Seq[Expression])(implicit scope: Scope): Seq[AggregateProjection] = {
    import com.lollypop.language.models._
    expressions map {
      case AllFields => die("Aggregation function or constant value expected")
      case f: BasicFieldRef => AggregateFieldProjection(name = f.alias || f.name, srcName = f.name)
      case fc@Count(Unique(args)) => AggregateFunctionProjection(name = fc.getNameOrDie, CountUnique(args.extract1))
      case aggFx: AggregateFunctionCall => AggregateFunctionProjection(name = aggFx.getNameOrDie, aggFx)
      case fc@NamedFunctionCall(functionName, args) =>
        val fx = scope.resolveInternalFunctionCall(functionName, args) match {
          case Some(f: AggregateFunctionCall) => f
          case Some(f) => die(s"Aggregation function expected near ${f.toSQL}")
          case None => die(s"Function '$functionName${args.map(_.toSQL).mkString("(", ", ", ")")}' not found")
        }
        AggregateFunctionProjection(name = fc.getNameOrDie, fx)
      case expression => dieUnsupportedEntity(expression, entityName = "expression")
    }
  }

  /**
   * Returns all columns referenced within the expressions
   * @param expressions the collection of expressions
   * @return the referenced [[TableColumn columns]]
   */
  private def getReferencedColumns(expressions: Seq[Expression], sourceColumns: Seq[TableColumn]): Seq[TableColumn] = {
    val results = (expressions flatMap {
      case AllFields => sourceColumns
      case f: BasicFieldRef => sourceColumns.find(_.name == f.name).toSeq
      case Count(Unique(args)) => getReferencedColumns(args.filterNot(_ == AllFields), sourceColumns)
      case InternalFunctionCall(_, args) => getReferencedColumns(args.filterNot(_ == AllFields), sourceColumns)
      case NamedFunctionCall(_, args) => getReferencedColumns(args.filterNot(_ == AllFields), sourceColumns)
      case _: Literal => Nil
      case expression => dieUnsupportedEntity(expression, entityName = "expression")
    }).distinct
    results
  }

  private def getReferencedJoinColumns(condition: Condition, tableAlias: String): List[String] = {
    toInequalities(condition) flatMap {
      case Inequality(j0: JoinFieldRef, j1: JoinFieldRef, _) if (j0.tableAlias contains tableAlias) && (j1.tableAlias contains tableAlias) => List(j0.name, j1.name)
      case Inequality(j: JoinFieldRef, _, _) if j.tableAlias contains tableAlias => List(j.name)
      case Inequality(_, j: JoinFieldRef, _) if j.tableAlias contains tableAlias => List(j.name)
      case _ => Nil
    } distinct
  }

  private def getTransformationColumns(expressions: Seq[Expression], sourceColumns: Seq[TableColumn]): Seq[TableColumn] = {
    def recurse(expression: Expression): Seq[TableColumn] = expression match {
      case AllFields => sourceColumns
      case b: BasicFieldRef => sourceColumns.find(_.name == b.name).map(_.copy(name = b.alias || b.name)).toSeq
      case _: JoinFieldRef => Nil
      case op@UnNest(field) =>
        field match {
          case BasicFieldRef(name) =>
            val column = sourceColumns.find(_.name == name) || op.dieNoSuchColumn(name)
            column.`type` match {
              case tt: TableType => tt.columns
              case _ => field.dieColumnIsNotTableType(name)
            }
          case expr => die(s"Table field reference expected near '${expr.toSQL}'")
        }
      case expr => Seq(TableColumn(name = expr.getNameOrDie, expr.returnType))
    }

    expressions flatMap recurse distinct
  }

  private def createJoinDevice(q: Queryable, joinColumnNames: String*)(implicit scope: Scope): (IOCost, RowCollection with CursorSupport) = {
    val (_, cost0, rc0) = LollypopVM.search(scope, q)
    val (cost1, rc1) = sort(rc0, joinColumnNames.map(column => OrderColumn(name = column, isAscending = true)))
    (cost0 ++ cost1, CursorSupport(rc1))
  }

}
