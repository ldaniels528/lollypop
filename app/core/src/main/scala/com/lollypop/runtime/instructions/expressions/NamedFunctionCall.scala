package com.lollypop.runtime.instructions.expressions

import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.models.Expression.implicits.LifestyleExpressions
import com.lollypop.language.models._
import com.lollypop.runtime.LollypopVM.implicits.{InstructionExtensions, InstructionSeqExtensions}
import com.lollypop.runtime.datatypes.ConstructorSupport
import com.lollypop.runtime.instructions.functions.{AnonymousNamedFunction, InternalFunctionCall, NamedFunction}
import com.lollypop.runtime.{Scope, __self__}
import lollypop.io.IOCost

/**
 * Represents a named function call
 * @param name the name of the function
 * @param args the function-call arguments
 */
case class NamedFunctionCall(name: String, args: List[Expression]) extends FunctionCall
  with RuntimeExpression with NamedExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    // handle the spread operator
    val myArgs = args match {
      case Seq(SpreadOperator(ArrayLiteral(value))) => value
      case Seq(SpreadOperator(Dictionary(value))) => value.map(_._2)
      case Seq(SpreadOperator(x)) => this.dieIllegalType(x)
      case x => x
    }

    try {
      scope.resolveAny(name, myArgs) match {
        case fx: LambdaFunction =>
          LambdaFunctionCall(fx, myArgs).execute(seed(scope, fx)) ~> { case (_, c, r) => (scope, c, r) }
        case fx: InternalFunctionCall => fx.execute(scope)
        case fx: Procedure => fx.code.execute(seed(scope, fx).withParameters(fx.params, myArgs))
        case fx: TypicalFunction => fx.code.execute(Scope(seed(scope, fx)).withParameters(fx.params, myArgs))
        case cs: ConstructorSupport[_] => myArgs.transform(scope) ~> { case (s, c, r) => (s, c, cs.construct(r)) }
        case _ => processInternalOps(name.f, myArgs)(scope)
      }
    } catch {
      case e: Throwable => this.die(e.getMessage, e)
    }
  }

  /**
   * Seeds the scope with a self reference to the host function
   * @param scope the base [[Scope scope]]
   * @param fx    the host function instance
   * @return the updated [[Scope scope]]
   * @example def factorial(n: Double) := iff(n <= 1.0, 1.0, n * __self__(n - 1.0))
   */
  private def seed(scope: Scope, fx: Any): Scope = {
    fx match {
      case nf: NamedFunction =>
        scope.withVariable(__self__, code = AnonymousNamedFunction(nf.name), isReadOnly = true)
      case _ => scope
    }
  }

  override def toSQL: String = s"$name(${args.map(_.toSQL).mkString(",")})"

}

/**
 * Named Function Call Companion
 * @author lawrence.daniels@gmail.com
 */
object NamedFunctionCall {

  /**
   * Creates a new function-call
   * @param name  the name of the function
   * @param items the function-call arguments
   * @return a new [[NamedFunctionCall function-call]]
   */
  def apply(name: String, items: Expression*) = new NamedFunctionCall(name, items.toList)

}