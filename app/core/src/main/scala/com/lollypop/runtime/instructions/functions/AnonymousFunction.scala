package com.lollypop.runtime.instructions.functions

import com.lollypop.language._
import com.lollypop.language.models._
import com.lollypop.runtime.conversions.ScalaConversion
import com.lollypop.runtime.instructions.RuntimeInstruction
import com.lollypop.runtime.instructions.expressions.LambdaFunctionCall
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
    params.length match {
      case 0 => new AnonymousFunction.F0(this)
      case 1 => new AnonymousFunction.F1(this)
      case 2 => new AnonymousFunction.F2(this)
      case 3 => new AnonymousFunction.F3(this)
      case 4 => new AnonymousFunction.F4(this)
      case 5 => new AnonymousFunction.F5(this)
      case 6 => new AnonymousFunction.F6(this)
      case 7 => new AnonymousFunction.F7(this)
      case 8 => new AnonymousFunction.F8(this)
      case 9 => new AnonymousFunction.F9(this)
      case 10 => new AnonymousFunction.F10(this)
      case 11 => new AnonymousFunction.F11(this)
      case 12 => new AnonymousFunction.F12(this)
      case 13 => new AnonymousFunction.F13(this)
      case 14 => new AnonymousFunction.F14(this)
      case 15 => new AnonymousFunction.F15(this)
      case 16 => new AnonymousFunction.F16(this)
      case 17 => new AnonymousFunction.F17(this)
      case 18 => new AnonymousFunction.F18(this)
      case 19 => new AnonymousFunction.F19(this)
      case 20 => new AnonymousFunction.F20(this)
      case 21 => new AnonymousFunction.F21(this)
      case 22 => new AnonymousFunction.F22(this)
      case x => die(s"Too many parameters ($x > 22)")
    }
  }

  override def toSQL: String = Seq(params.map(_.toSQL).mkString("(", ", ", ")"), "=>", code.toSQL).mkString(" ")

  def updateScope(scope: Scope): Scope = {
    this.origin = Some(scope)
    scope
  }

}

object AnonymousFunction extends AnonymousFunctionSymbol {

  private def invoke(af: AnonymousFunction)(args: Any*): Object = {
    val scope0 = af.origin || Scope()
    val scope1 = if (args.isEmpty) scope0 else scope0.withArguments(af.params, args)
    af.code.execute(scope1)._3.asInstanceOf[Object]
  }

  class F0(af: AnonymousFunction) extends scala.Function0[Object] {
    def apply(): Object = invoke(af)()
  }

  class F1(af: AnonymousFunction) extends scala.Function1[Object, Object] {
    def apply(a: Object): Object = invoke(af)(a)
  }

  class F2(af: AnonymousFunction) extends scala.Function2[Object, Object, Object] {
    def apply(a: Object, b: Object): Object = invoke(af)(a, b)
  }

  class F3(af: AnonymousFunction) extends scala.Function3[Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object): Object = invoke(af)(a, b, c)
  }

  class F4(af: AnonymousFunction) extends scala.Function4[Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object): Object = invoke(af)(a, b, c, d)
  }

  class F5(af: AnonymousFunction) extends scala.Function5[Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object): Object = invoke(af)(a, b, c, d, e)
  }

  class F6(af: AnonymousFunction) extends scala.Function6[Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object): Object = invoke(af)(a, b, c, d, e, f)
  }

  class F7(af: AnonymousFunction) extends scala.Function7[Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g)
    }
  }

  class F8(af: AnonymousFunction) extends scala.Function8[Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h)
    }
  }

  class F9(af: AnonymousFunction) extends scala.Function9[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i)
    }
  }

  class F10(af: AnonymousFunction) extends scala.Function10[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object, j: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j)
    }
  }

  class F11(af: AnonymousFunction) extends scala.Function11[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object,
              j: Object, k: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k)
    }
  }

  class F12(af: AnonymousFunction) extends scala.Function12[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object,
              g: Object, h: Object, i: Object, j: Object, k: Object, l: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l)
    }
  }

  class F13(af: AnonymousFunction) extends scala.Function13[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object,
              h: Object, i: Object, j: Object, k: Object, l: Object, m: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m)
    }
  }

  class F14(af: AnonymousFunction) extends scala.Function14[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object,
              h: Object, i: Object, j: Object, k: Object, l: Object, m: Object, n: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
    }
  }

  class F15(af: AnonymousFunction) extends scala.Function15[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object,
              i: Object, j: Object, k: Object, l: Object, m: Object, n: Object, o: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
    }
  }

  class F16(af: AnonymousFunction) extends scala.Function16[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object,
              i: Object, j: Object, k: Object, l: Object, m: Object, n: Object, o: Object, p: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
    }
  }

  class F17(af: AnonymousFunction) extends scala.Function17[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object,
              j: Object, k: Object, l: Object, m: Object, n: Object, o: Object, p: Object, q: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
    }
  }

  class F18(af: AnonymousFunction) extends scala.Function18[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object,
              j: Object, k: Object, l: Object, m: Object, n: Object, o: Object, p: Object, q: Object, r: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
    }
  }

  class F19(af: AnonymousFunction) extends scala.Function19[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object, j: Object,
              k: Object, l: Object, m: Object, n: Object, o: Object, p: Object, q: Object, r: Object, s: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
    }
  }

  class F20(af: AnonymousFunction) extends scala.Function20[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object, j: Object,
              k: Object, l: Object, m: Object, n: Object, o: Object, p: Object, q: Object, r: Object, s: Object, t: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
    }
  }

  class F21(af: AnonymousFunction) extends scala.Function21[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object, j: Object,
              k: Object, l: Object, m: Object, n: Object, o: Object, p: Object, q: Object, r: Object, s: Object, t: Object,
              u: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
    }
  }

  class F22(af: AnonymousFunction) extends scala.Function22[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object] {
    def apply(a: Object, b: Object, c: Object, d: Object, e: Object, f: Object, g: Object, h: Object, i: Object, j: Object,
              k: Object, l: Object, m: Object, n: Object, o: Object, p: Object, q: Object, r: Object, s: Object, t: Object,
              u: Object, v: Object): Object = {
      invoke(af)(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
    }
  }

}