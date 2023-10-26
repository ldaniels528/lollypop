package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.{Expression, FunctionCall, LambdaFunction, Queryable}
import com.qwery.runtime.instructions.functions.{AnonymousFunction, AnonymousNamedFunction, DataTypeConstructor}
import com.qwery.runtime.instructions.invocables.RuntimeInvokable
import com.qwery.runtime.{QweryVM, Scope}
import qwery.io.IOCost

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
        val (s, c, r) = QweryVM.execute(fxScope, code)
        (af.updateScope(s), c, r)
      case AnonymousNamedFunction(name) =>
        QweryVM.execute(scope, NamedFunctionCall(name, args))
      case dtf: DataTypeConstructor =>
        val (scopeA, costA, values) = QweryVM.transform(scope, args)
        val r = dtf.provider.construct(values)
        (scopeA, costA, r)
      case xx => xx.dieExpectedFunctionRef()
    }
  }

  override def toSQL: String = Seq(fx.toSQL, args.map(_.toSQL).mkString("(", ", ", ")")).mkString

}
