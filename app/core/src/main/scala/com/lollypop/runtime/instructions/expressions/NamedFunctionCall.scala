package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.Expression.implicits.LifestyleExpressions
import com.lollypop.language.models._
import com.lollypop.runtime.LollypopVM.implicits.{InstructionExtensions, InstructionSeqExtensions}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.ConstructorSupport
import com.lollypop.runtime.instructions.functions.InternalFunctionCall
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
      val result = scope.resolveAny(name, myArgs) match {
        case fx: LambdaFunction => LambdaFunctionCall(fx, myArgs).execute(scope)._3
        case fx: InternalFunctionCall => fx.execute(scope)._3
        case fx: Procedure => fx.code.execute(scope.withParameters(fx.params, myArgs))._3
        case fx: TypicalFunction => fx.code.execute(Scope(scope).withParameters(fx.params, myArgs))._3
        case cs: ConstructorSupport[_] => cs.construct(myArgs.transform(scope)._3)
        case _ => processInternalOps(name.f, myArgs)
      }
      (scope, IOCost.empty, result)
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