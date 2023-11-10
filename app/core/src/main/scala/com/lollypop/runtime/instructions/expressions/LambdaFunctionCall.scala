package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.{Expression, FunctionCall, LambdaFunction, Queryable}
import com.lollypop.runtime.LollypopVM.implicits.{InstructionExtensions, InstructionSeqExtensions}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.functions.{AnonymousFunction, AnonymousNamedFunction, DataTypeConstructor}
import com.lollypop.runtime.instructions.invocables.RuntimeInvokable
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
        val fxScope = origin_?.getOrElse(scope).withParameters(params, args)
        val (s, c, r) = code.execute(fxScope)
        (af.updateScope(s), c, r)
      case AnonymousNamedFunction(name) =>
        NamedFunctionCall(name, args).execute(scope)
      case dtf: DataTypeConstructor =>
        val (s, c, values) = args.transform(scope)
        (s, c, dtf.provider.construct(values))
      case xx => xx.dieExpectedFunctionRef()
    }
  }

  override def toSQL: String = Seq(fx.toSQL, args.map(_.toSQL).mkString("(", ", ", ")")).mkString

}
