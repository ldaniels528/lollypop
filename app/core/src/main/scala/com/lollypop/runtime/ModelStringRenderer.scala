package com.lollypop.runtime

import com.lollypop.database.server.LollypopServer
import com.lollypop.language.LollypopUniverse
import com.lollypop.language.models.{Atom, ColumnType, FieldRef, Literal}
import com.lollypop.runtime.datatypes.DataType
import com.lollypop.runtime.instructions.expressions.{ArrayLiteral, Dictionary, NamedFunctionCall, ScalarVariableRef}
import com.lollypop.runtime.instructions.queryables.TableVariableRef
import com.lollypop.util.DateHelper

/**
 * Model String Renderer
 */
object ModelStringRenderer {

  def render(value: Any): String = value match {
    case a: Array[_] => "Array" + a.map(render).mkString("(", ", ", ")")
    case ArrayLiteral(v) => "ArrayLiteral" + v.map(render).mkString("(", ", ", ")")
    case Atom(name) => qq(name) + ".a"
    case c: Char => "'" + c + "'"
    case c: ColumnType if c.isSimple => qq(c.name) + ".ct"
    case d: java.util.Date => "DateTime(" + qq(DateHelper.format(d)) + ")"
    case Dictionary(kv) => "Dictionary" + kv.map { case (k, v) => qq(k) -> render(v) }.mkString("(", ", ", ")")
    case FieldRef(name) => qq(name) + ".f"
    case l: Seq[_] if l.isEmpty => "Nil"
    case l: List[_] => "List" + l.map(render).mkString("(", ", ", ")")
    case l: Seq[_] => "Seq" + l.map(render).mkString("(", ", ", ")")
    case m: Map[_, _] => "Map" + m.map { case (k, v) => render(k) -> render(v) }.mkString("(", ", ", ")")
    case Left(v) => "Left(" + render(v) + ")"
    case Literal(v) => render(v) + ".v"
    case l: LollypopServer => s"LollypopServer(port=${render(l.port)}, ctx=${render(l.ctx)})"
    case l: LollypopUniverse => s"LollypopUniverse(escapeCharacter=${render(l.escapeCharacter)}, isServerMode=${render(l.isServerMode)})"
    case NamedFunctionCall(name, args) => qq(name) + ".fx" + args.map(render).mkString("(", ", ", ")")
    case None => "None"
    case Right(v) => "Right(" + render(v) + ")"
    case ScalarVariableRef(name) => "$(" + qq(name) + ")"
    case Some(v) => "Some(" + render(v) + ")"
    case s: String => qq(s)
    case s: StringBuffer => "StringBuffer(" + qq(s.toString) + ")"
    case s: StringBuilder => "StringBuilder(" + qq(s.toString) + ")"
    case TableVariableRef(name) => "@@(" + qq(name) + ")"
    case x: Product => x.asProductString
    case y: DataType => y.getClass.getSimpleName ~> { s => if (s.endsWith("$")) s.dropRight(1) else s }
    case z => String.valueOf(z)
  }

  private def qq(s: String) = s"\"$s\""

  final implicit class ModelStringRendering(val model: Any) extends AnyVal {
    @inline
    def asModelString: String = render(model)
  }

  final implicit class ProductStringRendering(val product: Product) extends AnyVal {
    @inline
    def asProductString: String = {
      product.getClass.getSimpleName + (product.productElementNames zip product.productIterator)
        .map { case (name, value) =>
          s"$name=${render(value)}"
        }.mkString("(", ", ", ")")
    }
  }

}
