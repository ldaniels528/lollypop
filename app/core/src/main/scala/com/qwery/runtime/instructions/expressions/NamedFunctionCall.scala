package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.Expression.implicits.LifestyleExpressions
import com.qwery.language.models._
import com.qwery.runtime.datatypes.ConstructorSupport
import com.qwery.runtime.instructions.functions.InternalFunctionCall
import com.qwery.runtime.{QweryVM, Scope}

/**
 * Represents a named function call
 * @param name the name of the function
 * @param args the function-call arguments
 */
case class NamedFunctionCall(name: String, args: List[Expression]) extends FunctionCall
  with RuntimeExpression with NamedExpression {

  override def evaluate()(implicit scope: Scope): Any = {
    // handle the spread operator
    val myArgs = args match {
      case Seq(SpreadOperator(ArrayLiteral(value))) => value
      case Seq(SpreadOperator(Dictionary(value))) => value.map(_._2)
      case Seq(SpreadOperator(x)) => this.dieIllegalType(x)
      case x => x
    }

    try {
      scope.resolveAny(name, myArgs) match {
        case fx: LambdaFunction => QweryVM.execute(scope, LambdaFunctionCall(fx, myArgs))._3
        case fx: InternalFunctionCall => QweryVM.execute(scope, fx)._3
        case fx: Procedure => QweryVM.execute(scope.withParameters(fx.params, myArgs), fx.code)._3
        case fx: TypicalFunction => QweryVM.execute(Scope(scope).withParameters(fx.params, myArgs), fx.code)._3
        case cs: ConstructorSupport[_] => cs.construct(QweryVM.evaluate(scope, myArgs))
        case _ => processInternalOps(name.f, myArgs)
      }
    } catch {
      case e: Throwable => this.die(e.getMessage, e)
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