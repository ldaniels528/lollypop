package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.{Expression, FunctionCall, LambdaFunction, Queryable}
import com.lollypop.runtime.instructions.functions.{AnonymousFunction, AnonymousNamedFunction, DataTypeConstructor}
import com.lollypop.runtime.instructions.invocables.RuntimeInvokable
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost

/**
 * Represents a lambda function invocation
 * @param fx   the [[LambdaFunction lambda function]]
 * @param args the function-call arguments
 * @example ((n: Int) => n * (n + 1))(5)
 */
case class LambdaFunctionCall(fx: LambdaFunction, args: List[Expression]) extends FunctionCall
  with RuntimeInvokable with Expression with Queryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    fx match {
      case af@AnonymousFunction(params, code, origin_?) =>
        val fxScope = origin_?.getOrElse(Scope(parentScope = scope)).withParameters(params, args)
        val (s, c, r) = LollypopVM.execute(fxScope, code)
        (af.updateScope(s), c, r)
      case AnonymousNamedFunction(name) =>
        LollypopVM.execute(scope, NamedFunctionCall(name, args))
      case dtf: DataTypeConstructor =>
        val (scopeA, costA, values) = LollypopVM.transform(scope, args)
        val r = dtf.provider.construct(values)
        (scopeA, costA, r)
      case xx => xx.dieExpectedFunctionRef()
    }
  }

  override def toSQL: String = Seq(fx.toSQL, args.map(_.toSQL).mkString("(", ", ", ")")).mkString

}
