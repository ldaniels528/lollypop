package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models._
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.{QMap, Row, RowCollection}
import com.lollypop.runtime.instructions.RuntimeInstruction
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassInstanceSugar
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Represents a run-time Expression
 */
trait RuntimeExpression extends Expression with RuntimeInstruction {

  def processInternalOps(host: Expression, args: List[Expression])(implicit scope: Scope): (Scope, IOCost, Any) = {
    val (sa, ca, instance) = host.execute(scope)
    val (sb, cb, values) = args.transform(sa)
    val tuple = this
    val result = instance match {
      // array(5) | ['A' to 'Z'](8, 19)
      case array: Array[_] =>
        values match {
          case List(a) => array(Int32Type.convert(a))
          case List(a, b) => array.slice(Int32Type.convert(a), Int32Type.convert(b))
          case xxx => tuple.dieArgumentMismatch(args = xxx.size, minArgs = 1, maxArgs = 2)
        }
      // device(9875) | device(9500, 9875)
      case rc: RowCollection =>
        values match {
          case List(a) => rc.apply(Int64Type.convert(a))
          case List(a, b) => rc.slice(Int64Type.convert(a), Int64Type.convert(b))
          case xxx => tuple.dieArgumentMismatch(args = xxx.size, minArgs = 1, maxArgs = 2)
        }
      // anonymousFx('a', 'b', 'c')
      case fx: LambdaFunction => fx.call(args).execute(sb)._3
      // ({ symbol: 'T', exchange: 'NYSE', lastSale: 22.77 })(0)
      case row: Row =>
        values match {
          case List(a) => row.fields(Int32Type.convert(a)).value.orNull
          case List(a, b) => row.fields.slice(Int32Type.convert(a), Int32Type.convert(b)).map(_.value.orNull).toArray
          case xxx => tuple.dieArgumentMismatch(args = xxx.size, minArgs = 1, maxArgs = 2)
        }
      // mapping(8)
      case mapping: QMap[_, _] =>
        values match {
          case List(a) => mapping.toSeq(Int32Type.convert(a))
          case xxx => tuple.dieArgumentMismatch(args = xxx.size, minArgs = 1, maxArgs = 2)
        }
      // list(80) | list(1, 5)
      case seq: Seq[_] =>
        values match {
          case List(a) => seq(Int32Type.convert(a))
          case List(a, b) => seq.slice(Int32Type.convert(a), Int32Type.convert(b))
          case xxx => tuple.dieArgumentMismatch(args = xxx.size, minArgs = 1, maxArgs = 2)
        }
      // 'Hello World'(4) | 'Hello World'(2, 6)
      case string: String =>
        values match {
          case List(a) => string.charAt(Int32Type.convert(a))
          case List(a, b) => string.slice(Int32Type.convert(a), Int32Type.convert(b))
          case xxx => tuple.dieArgumentMismatch(args = xxx.size, minArgs = 1, maxArgs = 2)
        }
      // instance.apply(1, 78, 'H')
      case inst => inst.invokeMethod(name = "apply", params = args)(sb)
    }
    (sb, ca ++ cb, result)
  }

}
