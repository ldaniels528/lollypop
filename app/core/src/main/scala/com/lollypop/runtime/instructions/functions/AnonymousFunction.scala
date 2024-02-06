package com.lollypop.runtime.instructions.functions

import com.lollypop.language._
import com.lollypop.language.models._
import com.lollypop.runtime.conversions.ScalaConversion
import com.lollypop.runtime.instructions.RuntimeInstruction
import com.lollypop.runtime.instructions.expressions.LambdaFunctionCall
import com.lollypop.runtime.instructions.functions.AnonymousFunction.constructors
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Represents a nameless (lambda) function
 * @param params the collection of function [[ParameterLike parameters]]
 * @param code   the function [[Instruction code]]
 * @param origin the originating [[Scope scope]]
 * @example (n: Int) => n + 1
 * @example n => n + 1
 */
case class AnonymousFunction(params: Seq[ParameterLike], code: Instruction, var origin: Option[Scope] = None)
  extends TypicalFunction with LambdaFunction with ScalaConversion
    with RuntimeInstruction
    with ContainerInstruction {

  override def call(args: List[Expression]): LambdaFunctionCall = LambdaFunctionCall(this, args)

  override def execute()(implicit scope: Scope): (Scope, IOCost, AnonymousFunction) = {
    // capture the scope at the time of the lambda's creation (first execution)
    if (origin.isEmpty) updateScope(scope)
    (scope, IOCost.empty, this)
  }

  override def toScala: AnyRef = {
    val index = params.length
    assert(index <= 22, die(s"Too many parameters ($index > 22)"))
    constructors(index)(this)
  }

  override def toSQL: String = Seq(params.map(_.toSQL).mkString("(", ", ", ")"), "=>", code.toSQL).mkString(" ")

  def updateScope(scope: Scope): Scope = {
    this.origin = Some(scope)
    scope
  }

}

object AnonymousFunction extends AnonymousFunctionSymbol {
  private val constructors: Array[AnonymousFunction => AnyRef] = Array(
    f => new AnonymousFunction.F0(f),
    f => new AnonymousFunction.F1(f),
    f => new AnonymousFunction.F2(f),
    f => new AnonymousFunction.F3(f),
    f => new AnonymousFunction.F4(f),
    f => new AnonymousFunction.F5(f),
    f => new AnonymousFunction.F6(f),
    f => new AnonymousFunction.F7(f),
    f => new AnonymousFunction.F8(f),
    f => new AnonymousFunction.F9(f),
    f => new AnonymousFunction.F10(f),
    f => new AnonymousFunction.F11(f),
    f => new AnonymousFunction.F12(f),
    f => new AnonymousFunction.F13(f),
    f => new AnonymousFunction.F14(f),
    f => new AnonymousFunction.F15(f),
    f => new AnonymousFunction.F16(f),
    f => new AnonymousFunction.F17(f),
    f => new AnonymousFunction.F18(f),
    f => new AnonymousFunction.F19(f),
    f => new AnonymousFunction.F20(f),
    f => new AnonymousFunction.F21(f),
    f => new AnonymousFunction.F22(f))

  private def invoke(af: AnonymousFunction)(args: Any*): Object = {
    val scope0 = af.origin || Scope()
    val scope1 = if (args.isEmpty) scope0 else scope0.withArguments(af.params, args)
    af.code.execute(scope1)._3.asInstanceOf[Object]
  }

  private class F0(af: AnonymousFunction) extends scala.Function0[Object] {
    def apply(): Object = invoke(af)()
  }

  private class F1(af: AnonymousFunction) extends scala.Function1[Object, Object] {
    def apply(a: Object): Object = invoke(af)(a)
  }

  private class F2(af: AnonymousFunction) extends scala.Function2[Object, Object, Object] {
    def apply(a: Object, b: Object): Object = invoke(af)(a, b)
  }

  private class F3(af: AnonymousFunction) extends scala.Function3[Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object): Object = invoke(af)(a, b, c)
  }

  private class F4(af: AnonymousFunction) extends scala.Function4[Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object): Object = invoke(af)(a, b, c, d)
  }

  private class F5(af: AnonymousFunction) extends scala.Function5[Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object): Object = invoke(af)(a, b, c, d, e)
  }

  private class F6(af: AnonymousFunction) extends scala.Function6[Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object): Object = invoke(af)(a, b, c, d, e, f)
  }

  private class F7(af: AnonymousFunction) extends scala.Function7[Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g)
    }
  }

  private class F8(af: AnonymousFunction) extends scala.Function8[Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h)
    }
  }

  private class F9(af: AnonymousFunction) extends scala.Function9[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i)
    }
  }

  private class F10(af: AnonymousFunction) extends scala.Function10[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object, j: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j)
    }
  }

  private class F11(af: AnonymousFunction) extends scala.Function11[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object,
              j: Object, k: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k)
    }
  }

  private class F12(af: AnonymousFunction) extends scala.Function12[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object,
              g: Object, h: Object, i: Object, j: Object, k: Object, l: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l)
    }
  }

  private class F13(af: AnonymousFunction) extends scala.Function13[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object,
              h: Object, i: Object, j: Object, k: Object, l: Object, m: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m)
    }
  }

  private class F14(af: AnonymousFunction) extends scala.Function14[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object,
              h: Object, i: Object, j: Object, k: Object, l: Object, m: Object, n: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
    }
  }

  private class F15(af: AnonymousFunction) extends scala.Function15[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object,
              i: Object, j: Object, k: Object, l: Object, m: Object, n: Object, o: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
    }
  }

  private class F16(af: AnonymousFunction) extends scala.Function16[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object,
              i: Object, j: Object, k: Object, l: Object, m: Object, n: Object, o: Object, p: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
    }
  }

  private class F17(af: AnonymousFunction) extends scala.Function17[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object,
              j: Object, k: Object, l: Object, m: Object, n: Object, o: Object, p: Object, q: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
    }
  }

  private class F18(af: AnonymousFunction) extends scala.Function18[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object,
              j: Object, k: Object, l: Object, m: Object, n: Object, o: Object, p: Object, q: Object, r: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
    }
  }

  private class F19(af: AnonymousFunction) extends scala.Function19[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object, j: Object,
              k: Object, l: Object, m: Object, n: Object, o: Object, p: Object, q: Object, r: Object, s: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
    }
  }

  private class F20(af: AnonymousFunction) extends scala.Function20[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object, j: Object,
              k: Object, l: Object, m: Object, n: Object, o: Object, p: Object, q: Object, r: Object, s: Object, t: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
    }
  }

  private class F21(af: AnonymousFunction) extends scala.Function21[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object, j: Object,
              k: Object, l: Object, m: Object, n: Object, o: Object, p: Object, q: Object, r: Object, s: Object, t: Object,
              u: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
    }
  }

  private class F22(af: AnonymousFunction) extends scala.Function22[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object, j: Object,
              k: Object, l: Object, m: Object, n: Object, o: Object, p: Object, q: Object, r: Object, s: Object, t: Object,
              u: Object, v: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
    }
  }

}