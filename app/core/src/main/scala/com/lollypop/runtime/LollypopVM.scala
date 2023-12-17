package com.lollypop.runtime

import com.lollypop.LollypopException
import com.lollypop.language._
import com.lollypop.language.models._
import com.lollypop.runtime.devices._
import com.lollypop.runtime.instructions.infrastructure.Macro
import com.lollypop.runtime.instructions.invocables.{SetAnyVariable, WhenEver}
import com.lollypop.runtime.instructions.{MacroLanguageParser, RuntimeInstruction}
import com.lollypop.runtime.plastics.RuntimeClass
import lollypop.io.IOCost

import scala.language.postfixOps

/**
 * Lollypop Virtual Machine
 */
object LollypopVM {
  val rootScope: Scope = LollypopUniverse().createRootScope()

  ///////////////////////////////////////////////////////////////////////////////////////////
  //      EVALUATION METHODS
  ///////////////////////////////////////////////////////////////////////////////////////////

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
      case q: LollypopException => throw q
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
   * Executes an SQL query
   * @param scope the [[Scope scope]]
   * @param sql   the query string
   * @return the potentially updated [[Scope scope]] and the resulting [[RowCollection row collection]]
   */
  def searchSQL(scope: Scope, sql: String): (Scope, IOCost, RowCollection) = {
    scope.getCompiler.compile(sql).search(scope)
  }

  ///////////////////////////////////////////////////////////////////////////////////////////
  //      UTILITY METHODS
  ///////////////////////////////////////////////////////////////////////////////////////////

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

}