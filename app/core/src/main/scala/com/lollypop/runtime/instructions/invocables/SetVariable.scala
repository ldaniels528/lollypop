package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, RichAliasable}
import com.lollypop.language.models._
import com.lollypop.runtime.devices.{QMap, Row}
import com.lollypop.runtime.instructions.expressions.Infix
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassInstanceSugar
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import scala.collection.mutable

sealed trait SetVariable extends RuntimeInvokable with ScopeModification

object SetVariable extends InvokableParser {
  val template: String = "set %U:assignments"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "set",
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Sets the value of a variable",
    example =
      """|set x = { a: { b: { c : 98 } } }
         |x.a.b.c
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[SetVariable] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      val instructions: List[SetVariable] = params.assignments("assignments")
        .map { case (name, instruction) => SetAnyVariable(name.f, instruction) }
      instructions match {
        case List(instruction) => Some(instruction)
        case setters => Some(ScopeModificationBlock(setters))
      }
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "set"
}

/**
 * Represents a scalar/table variable assignment
 * @param ref         the given variable reference
 * @param instruction the given [[Instruction dataset]]
 * @example {{{ set customers = ( select * from Customers where deptId = 31 ) }}}
 */
case class SetAnyVariable(ref: Expression, instruction: Instruction) extends SetVariable {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope.setVariable(ref.getNameOrDie, instruction), IOCost.empty, null)
  }

  override def toSQL: String = instruction match {
    case queryable: Queryable => s"set ${ref.toSQL} = (${queryable.toSQL})"
    case other => s"set ${ref.toSQL} = ${other.toSQL}"
  }
}

/**
 * Represents a scope-modifying code block
 * @param instructions the scope-modifying [[ScopeModification instruction]]
 * @example {{{
 *  set x = 3, y = 6, z = 9, a += 0.01
 * }}}
 */
case class ScopeModificationBlock(instructions: List[ScopeModification]) extends CodeBlock
  with SetVariable with ScopeModification {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val s = instructions.foldLeft[Scope](scope) {
      case (aggScope, SetAnyVariable(name, instruction)) =>
        aggScope.setVariable(name.getNameOrDie, instruction)
      case (aggScope, instruction) =>
        LollypopVM.execute(aggScope, instruction)._1
    }
    (s, IOCost.empty, null)
  }

  override def toSQL: String = instructions.map(_.toSQL).mkString(", ")
}

object ScopeModificationBlock {
  def apply(ops: ScopeModification*) = new ScopeModificationBlock(ops.toList)
}

case class SetVariableExpression(ref: Expression, expression: Expression) extends SetVariable with ModificationExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    ref match {
      case FieldRef(name) => (scope.setVariable(name, expression), IOCost.empty, null)
      case VariableRef(name) => (scope.setVariable(name, expression), IOCost.empty, null)
      case setter =>
        // get the variable/field names of each segment
        val names = unpack(setter).map {
          case FieldRef(name) => name
          case VariableRef(name) => name
          case expr => expr.dieIllegalType()
        }
        // iteratively resolve (the value of) each segment
        if (names.size < 2) setter.dieIllegalType() else {
          // get the instance with the setter
          val inst = names.drop(1).dropRight(1).foldLeft[Any](scope.resolve(names.head).orNull) {
            case (p: Product, fieldName) =>
              Map[String, Any](p.getClass.getDeclaredFields.map(_.getName) zip p.productIterator: _*)
                .getOrElse(fieldName, p.invokeField(fieldName))
            case (m: QMap[String, _], fieldName) => m.get(fieldName).orNull
            case (m: java.util.Map[String, Any], fieldName) => m.get(fieldName)
            case (obj, fieldName) => obj.invokeField(fieldName)
          }
          // invoke the segment's setter
          (inst, names.last) match {
            case (r: Row, fieldName) =>
              (r.getField(fieldName) || expression.dieNoSuchColumn(fieldName)).value = Option(LollypopVM.execute(scope, expression)._3)
            case (p: Product, fieldName) =>
              p.invokeMethod(name = s"${fieldName}_$$eq", params = List(expression))
            case (m: java.util.Map[String, Any], fieldName) =>
              m.put(fieldName, LollypopVM.execute(scope, expression)._3)
            case (m: mutable.Map[String, Any], fieldName) =>
              m.put(fieldName, LollypopVM.execute(scope, expression)._3)
            case _ => setter.dieIllegalType()
          }
          (scope, IOCost.empty, null)
        }
    }
  }

  private def unpack(expression: Expression): List[Expression] = expression match {
    case Infix(a, b) => unpack(a) ::: unpack(b)
    case a => List(a)
  }

  override def toSQL: String = Seq(ref.toSQL, "=", expression.toSQL).mkString(" ")

}

object SetVariableExpression extends ExpressionChainParser {

  override def help: List[HelpDoc] = Nil

  override def parseExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[SetVariableExpression] = {
    if (ts nextIf "=") compiler.nextExpression(ts).map(SetVariableExpression(host, _)) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "="

}
