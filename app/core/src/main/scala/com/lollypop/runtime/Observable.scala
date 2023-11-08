package com.lollypop.runtime

import com.lollypop.language.models._
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.instructions.expressions.NamedFunctionCall

import scala.language.implicitConversions

/**
 * Represents an Observable entity
 */
sealed trait Observable {

  /**
   * @return the [[Instruction code]] to execute when the Observable is triggered
   */
  def code: Instruction

  /**
   * Executes the Observable's code
   * @param instruction the [[Instruction instruction]] that triggered the event
   * @param name        the optional name of the field or variable
   * @param scope       the implicit [[Scope scope]]
   * @param result      the results of the execution/computation
   */
  def execute(instruction: Instruction, name: String, result: Any)(implicit scope: Scope): Unit

  /**
   * Indicates whether this Observable should handle the event
   * @param instruction the executed [[Instruction instruction]]
   * @param name        the optional name of the field or variable
   * @return true, if this Observable knows how to handle the event
   */
  def understands(instruction: Instruction, name: String): Boolean

}

/**
 * Observable Companion
 */
object Observable {

  /**
   * Creates a new Observable
   * @param expression the observable entity
   * @param code       the [[Instruction code]] to execute when the Observable is triggered
   * @return a new [[Observable]]
   */
  def apply(expression: Expression, code: Instruction): Observable = {
    expression match {
      case condition: Condition =>
        val refs = Inequality.toInequalities(condition).flatMap(extractVariableNames).distinct
        if (refs.isEmpty) condition.die(s"Could not determine variable to listen to: ${condition.toSQL}")
        ObservableCondition(condition, refs, code)
      case Literal(regEx: String) => ObservableRegEx(regEx, code)
      case other => other.die("A condition or regular expression was expected")
    }
  }

  def unapply(observable: Observable): Option[Instruction] = Option(observable.code)

  private def extractVariableNames(expression: Expression): List[String] = expression match {
    case FieldRef(name) => List(name)
    case Inequality(a, b, _) => extractVariableNames(a) ::: extractVariableNames(b)
    case NamedFunctionCall(_, args) => args.flatMap(extractVariableNames)
    case UnaryOperation(a) => extractVariableNames(a)
    case BinaryOperation(a, b) => extractVariableNames(a) ::: extractVariableNames(b)
    case ModificationExpression(a, _) => extractVariableNames(a)
    case VariableRef(name) => List(name)
    case _ => Nil
  }

  /**
   * Represents an Observable Condition
   * @param condition the [[Condition condition]] to observe
   * @param refs      the field/variable references
   * @param code      the [[Instruction code]] to execute when the Observable is triggered
   */
  private case class ObservableCondition(condition: Condition, refs: List[String], code: Instruction) extends Observable {
    override def execute(instruction: Instruction, name: String, result: Any)(implicit scope0: Scope): Unit = {
      // is the whenever condition satisfied?
      val (scope1, cost1, result1) = condition.execute(scope0)
      // if so, execute the code
      if (result1 == true) code.execute(scope1)
    }

    override def understands(instruction: Instruction, name: String): Boolean = refs contains name
  }

  /**
   * Represents an Observable Regular Expression
   * @param pattern the regular expression to observe
   * @param code    the [[Instruction code]] to execute when the Observable is triggered
   */
  private case class ObservableRegEx(pattern: String, code: Instruction) extends Observable {
    override def execute(instruction: Instruction, name: String, result: Any)(implicit scope0: Scope): Unit = {
      val scope1 = scope0
        .withVariable(__INSTRUCTION__, value = instruction.toSQL)
        .withVariable(__RETURNED__, value = result)
      code.execute(scope1)
    }

    override def understands(instruction: Instruction, name: String): Boolean = instruction.toSQL.matches(pattern)
  }

}

