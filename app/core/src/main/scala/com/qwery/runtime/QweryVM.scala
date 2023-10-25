package com.qwery.runtime

import com.qwery.implicits.MagicImplicits
import com.qwery.language.models.Expression.implicits.RichAliasable
import com.qwery.language.models._
import com.qwery.language.{QweryUniverse, dieIllegalType, dieNoSuchColumn}
import com.qwery.runtime.QweryVM.implicits.RichScalaAny
import com.qwery.runtime.datatypes.Inferences
import com.qwery.runtime.datatypes.Inferences.fromValue
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.RowCollectionZoo._
import com.qwery.runtime.devices._
import com.qwery.runtime.instructions.infrastructure.Macro
import com.qwery.runtime.instructions.invocables.{SetAnyVariable, WhenEver}
import com.qwery.runtime.instructions.queryables.TableRendering
import com.qwery.runtime.instructions.{MacroLanguageParser, RuntimeInstruction}
import com.qwery.{QweryException, die}
import qwery.io.IOCost

import scala.annotation.tailrec
import scala.language.postfixOps

/**
 * Qwery Virtual Machine
 */
object QweryVM {
  private val pureScope = QweryUniverse().createRootScope()
  private val resultName = "result"

  ///////////////////////////////////////////////////////////////////////////////////////////
  //      EVALUATION METHODS
  ///////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Evaluates a collection of expressions
   * @param expressions the collection of [[Expression expressions]] to evaluate
   * @return the collection of results
   */
  def evaluate(scope: Scope, expressions: Seq[Expression]): List[Any] = {
    expressions.map(execute(scope, _)._3).toList
  }

  /**
   * Evaluates a pure expression
   * @param instruction the [[Instruction instruction]] to evaluate
   * @return the resulting value
   */
  def evaluatePure(instruction: Instruction): Any = execute(pureScope, instruction)._3

  /**
   * Executes an instruction
   * @param scope       the [[Scope scope]]
   * @param instruction the [[Instruction instruction]]
   * @return a tuple containing the updated [[Scope scope]], [[IOCost]] and the return value
   */
  def execute(scope: Scope, instruction: Instruction): (Scope, IOCost, Any) = {
    val startTime = System.nanoTime()
    val (scopeA: Scope, costA: IOCost, resultA) = try instruction match {
      case i: RuntimeInstruction => i.execute()(scope)
      case z => (scope, IOCost.empty, z)
    } catch {
      case q: QweryException => throw q
      case t: Throwable => instruction.die(t.getMessage, t)
    }

    // post-processing of external components, MACROs and variables
    val (scopeB: Scope, costB: IOCost, resultB) = resultA.unwrapOptions match {
      case c: ExternalComponent => RuntimeClass.loadExternalComponent(c)(scopeA) ~> { case (s, c, r) => (s, costA ++ c, r) }
      case m: Macro => MacroLanguageParser.registerMacro(m); (scopeA, costA, m)
      case v: ValueReference => (scopeA.withVariable(v), costA, true)
      case z => (scopeA, costA, z)
    }

    // allow tracers to analyze the stats
    val elapsedTime = (System.nanoTime() - startTime) / 1e+6
    scope.getTracers.foreach(_(instruction, scopeB, resultB, elapsedTime))

    // finally, update the observers
    try executeObservables(instruction, scopeB, resultB) ~> { case (s, r) => (s, costB, r) } catch {
      case t: Throwable => instruction.die(t.getMessage, t)
    }
  }

  /**
   * Executes a SQL statement or query
   * @param scope the [[Scope scope]]
   * @param sql   the SQL statement or query
   * @return a tuple containing the updated [[Scope scope]], [[IOCost]] and the return value
   */
  def executeSQL(scope: Scope, sql: String): (Scope, IOCost, Any) = {
    execute(scope, scope.getCompiler.compile(sql))
  }

  /**
   * Evaluates an [[Instruction instruction]]
   * @param scope       the [[Scope scope]]
   * @param instruction the [[Instruction instruction]] to evaluate
   * @return the potentially updated [[Scope scope]] and the resulting [[RowCollection block device]]
   */
  def search(scope: Scope, instruction: Instruction): (Scope, IOCost, RowCollection) = {
    execute(scope, instruction) match {
      case (aScope, aCost, rc: RowCollection) => (aScope, aCost, rc)
      case (aScope, aCost, rendering: TableRendering) => (aScope, aCost, rendering.toTable(scope))
      case (aScope, aCost, other) => (aScope, aCost, convertToTable(resultName, other))
    }
  }

  /**
   * Executes an SQL query
   * @param scope the [[Scope scope]]
   * @param sql   the query string
   * @return the potentially updated [[Scope scope]] and the resulting [[RowCollection block device]]
   */
  def searchSQL(scope: Scope, sql: String): (Scope, IOCost, RowCollection) = {
    search(scope, scope.getCompiler.compile(sql))
  }

  /**
   * Evaluates a collection of instructions
   * @param instructions the collection of [[Instruction instructions]] to execute
   * @return the tuple consisting of the [[Scope]], [[IOCost]] and the collection of results
   */
  def transform(scope0: Scope, instructions: List[Instruction]): (Scope, IOCost, List[Any]) = {
    instructions.foldLeft[(Scope, IOCost, List[Any])]((scope0, IOCost.empty, Nil)) {
      case ((scope, cost, list), op) => execute(scope, op) ~> { case (s, c, r) => (s, cost ++ c, list ::: List(r)) }
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////
  //      UTILITY METHODS
  ///////////////////////////////////////////////////////////////////////////////////////////

  def convertToTable(columnName: String, value: Any): RowCollection = {
    val device = createTempTable(columns = Seq(TableColumn(columnName, `type` = fromValue(value))), fixedRowCount = 1)
    device.insert(Map(columnName -> value).toRow(device))
    device
  }

  def convertToTable(collection: Seq[_]): RowCollection = {
    // determine the data types of the dictionary entries
    val rawColumns = collection map {
      case rc: RowCollection => rc.columns
      case row: Row => row.columns
      case dict: QMap[String, Any] =>
        dict.toSeq map { case (name, value) => TableColumn(name, `type` = Inferences.fromValue(value)) }
      case other => die(s"Expected a dictionary object, got ${Option(other).map(_.getClass.getName).orNull}")
    }

    // determine the best fit for each generated column type
    val columns = rawColumns.flatten.groupBy(_.name).toSeq.map { case (name, columns) =>
      TableColumn(name, `type` = Inferences.resolveType(columns.map(_.`type`): _*))
    }

    // write the data to the table
    val device = FileRowCollection(columns)
    val (fmd, rmd) = (FieldMetadata(), RowMetadata())
    collection foreach {
      case rc: RowCollection => device.insert(rc)
      case dict: QMap[String, Any] =>
        val row = Row(device.getLength, rmd, columns, fields = columns map { column =>
          Field(column.name, fmd, value = dict.get(column.name))
        })
        device.insert(row)
      case x => dieIllegalType(x)
    }
    device
  }

  def sort(collection: RowCollection, orderBy: Seq[OrderColumn]): (IOCost, RowCollection) = {
    if (orderBy.isEmpty) IOCost() -> collection else {
      val orderByColumn = orderBy.headOption
      val sortColumnID = collection.columns.indexWhere(col => orderByColumn.exists(_.name == col.name)) match {
        case -1 => dieNoSuchColumn(name = orderByColumn.map(_.name).orNull)
        case index => index
      }
      val cost = collection.sortInPlace(collection.readField(_, sortColumnID).value, isAscending = orderByColumn.exists(_.isAscending))
      cost -> collection
    }
  }

  /**
   * Evaluates all observables where a referenced variable has been modified or a trigger instruction has been executed.
   * @param instruction the executed [[Instruction instruction]]
   * @param scope       the [[Scope scope]]
   * @param result      the result of the executed [[Instruction instruction]]
   * @return a tuple of the [[Scope scope]] and the result
   */
  private def executeObservables(instruction: Instruction, scope: Scope, result: Any): (Scope, Any) = {

    def executeObservable(host: Instruction, scope0: Scope, name: String): Unit = {
      scope0.getObservables foreach {
        case obs if obs.understands(host, name) => obs.execute(host, name, result)(scope0)
        case _ =>
      }
    }

    if (!scope.isObserved && scope.getObservables.nonEmpty) {
      val scope0 = scope.withObserved(observed = true)
      instruction match {
        case _: CodeBlock =>
        case _: WhenEver =>
        case me: ModificationExpression => executeObservable(me, scope0, name = scope0.resolveReferenceName(me.ref))
        case sv: SetAnyVariable => executeObservable(sv, scope0, sv.ref.getNameOrDie)
        case xx => executeObservable(xx, scope0, name = "")
      }
    }
    (scope, result)
  }

  object implicits {

    final implicit class RichScalaAny(val item: Any) extends AnyVal {

      @inline def unwrapOptions: Any = {
        @tailrec
        def recurse(value: Any): Any = value match {
          case Some(v) => recurse(v)
          case None => null
          case v => v
        }

        recurse(item)
      }

    }

  }

}