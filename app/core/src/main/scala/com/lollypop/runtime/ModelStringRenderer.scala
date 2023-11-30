package com.lollypop.runtime

import com.lollypop.database.server.LollypopServer
import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.LollypopUniverse
import com.lollypop.language.models.{Atom, ColumnType, FieldRef, Literal}
import com.lollypop.runtime.datatypes.DataType
import com.lollypop.runtime.instructions.expressions.{ArrayLiteral, Dictionary, NamedFunctionCall, ScalarVariableRef}
import com.lollypop.runtime.instructions.queryables.TableVariableRef

/**
 * Model String Renderer
 */
object ModelStringRenderer {

  final implicit class ModelStringRendering(val model: Any) extends AnyVal {
    def asModelString: String = {
      def recurse(value: Any): String = value match {
        case a: Array[Any] => "Array" + a.map(recurse).mkString("(", ", ", ")")
        case ArrayLiteral(v) => "ArrayLiteral" + v.map(recurse).mkString("(", ", ", ")")
        case Atom(name) => qq(name) + ".a"
        case c: Char => "'" + c + "'"
        case c: ColumnType if c.isSimple => qq(c.name) + ".ct"
        case Dictionary(kv) => "Dictionary" + kv.map { case (k, v) => qq(k) -> recurse(v) }.mkString("(", ", ", ")")
        case FieldRef(name) => qq(name) + ".f"
        case l: Seq[_] if l.isEmpty => "Nil"
        case l: List[_] => "List" + l.map(recurse).mkString("(", ", ", ")")
        case l: Seq[_] => "Seq" + l.map(recurse).mkString("(", ", ", ")")
        case m: Map[_, _] => "Map" + m.map { case (k, v) => recurse(k) -> recurse(v) }.mkString("(", ", ", ")")
        case Left(v) => "Left(" + recurse(v) + ")"
        case Literal(v) => recurse(v) + ".v"
        case l: LollypopServer => s"LollypopServer(port=${l.port}, ctx=${l.ctx.asModelString})"
        case l: LollypopUniverse => s"LollypopUniverse(isServerMode=${l.isServerMode})"
        case NamedFunctionCall(name, args) => qq(name) + ".fx" + args.map(recurse).mkString("(", ", ", ")")
        case None => "None"
        case Right(v) => "Right(" + recurse(v) + ")"
        case ScalarVariableRef(name) => "$(" + qq(name) + ")"
        case Some(v) => "Some(" + recurse(v) + ")"
        case s: String => qq(s)
        case s: StringBuffer => "StringBuffer(" + qq(s.toString) + ")"
        case s: StringBuilder => "StringBuilder(" + qq(s.toString) + ")"
        case TableVariableRef(name) => "@@(" + qq(name) + ")"
        case x: Product => x.asProductString
        case y: DataType => y.getClass.getSimpleName ~> { s => if (s.endsWith("$")) s.dropRight(1) else s }
        case z => String.valueOf(z)
      }

      recurse(model)
    }

    private def qq(s: String) = s"\"$s\""

  }

  final implicit class ProductStringRendering(val product: Product) extends AnyVal {
    @inline
    def asProductString: String = {
      product.getClass.getSimpleName + (product.productElementNames zip product.productIterator)
        .map { case (name, value) =>
          s"$name=${value.asModelString}"
        }.mkString("(", ", ", ")")
    }
  }

}
