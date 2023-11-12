package com.lollypop.runtime.datatypes

import com.lollypop.die
import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.models._
import com.lollypop.language.{LollypopUniverse, dieUnsupportedType}
import com.lollypop.runtime.LollypopNative
import com.lollypop.runtime.instructions.expressions._
import com.lollypop.runtime.instructions.functions.Iff
import com.lollypop.runtime.instructions.invocables.{IF, Switch}
import com.lollypop.util.OptionHelper.OptionEnrichment

/**
 * Data Type Inferences
 */
trait Inferences {

  /**
   * Determines the equivalent data type to the given class
   * @param `class` the given [[Class class]]
   * @return the [[DataType data type]]
   */
  def fromClass(`class`: Class[_]): DataType = `class` match {
    case c if c.isArray =>
      fromClass(c.getComponentType) match {
        case Int8Type => VarBinaryType
        case CharType => VarCharType
        case _type => ArrayType(_type)
      }
    case c =>
      LollypopUniverse().dataTypeParsers.collectFirst {
        case t if t.getCompatibleType(c).nonEmpty => t.getCompatibleType(c).orNull
      } getOrElse dieUnsupportedType(c)
  }

  /**
   * Determines the equivalent data type for the given value
   * @param value the given value
   * @return the [[DataType data type]]
   */
  def fromValue(value: Any): DataType = value match {
    case Some(value) => fromValue(value)
    case None | null => AnyType
    case _: Boolean => BooleanType
    case a: Array[_] => fromClass(a.getClass.getComponentType) ~> {
      case Int8Type => VarBinaryType(a.length)
      case CharType => VarCharType(a.length)
      case _type => ArrayType(_type, capacity = Some(a.length))
    }
    case v =>
      LollypopUniverse().dataTypeParsers.collectFirst {
        case t if t.getCompatibleValue(v).nonEmpty => t.getCompatibleValue(v).orNull
      } getOrElse dieUnsupportedType(v)
  }

  def inferType(instruction: Instruction): DataType = {

    def ifA(onTrue: Instruction, onFalse: Option[Instruction]): DataType = resolveType((onTrue :: onFalse.toList).map(inferType): _*)

    def ifB(onTrue: Instruction, onFalse: Instruction): DataType = resolveType(inferType(onTrue), inferType(onFalse))

    def recurse(opCode: Instruction): DataType = opCode match {
      case q: LollypopNative => q.returnType
      // specific
      case ArrayFromRange(a, b) => ArrayType(componentType = resolveType(inferType(a), inferType(b)))
      case CodeBlock(ops) => ops.lastOption.map(recurse) || AnyType
      case IF(_, onTrue, onFalse) => ifA(onTrue, onFalse)
      case Iff(_, onTrue, onFalse) => ifB(onTrue, onFalse)
      case UnaryOperation(a) => resolveType(recurse(a))
      case BinaryOperation(a, b) => resolveType(recurse(a), recurse(b))
      case New(Atom(typeName), _, _) => AnyType(typeName)
      case Switch(_, cases) => resolveType(cases.map(_.result).map(recurse): _*)
      // generic
      case _: Queryable => TableType(columns = Nil)
      case _ => AnyType
    }

    recurse(instruction)
  }

  def fastTypeResolve(aa: Number, bb: Number): DataType = resolveType(fromValue(aa), fromValue(bb))

  def resolveType(types: DataType*): DataType = {
    val _types = types.distinct.filterNot(_ == AnyType)
    if (_types.isEmpty) AnyType else {
      _types.reduce[DataType] {
        case (a, b) if a.name == b.name => if (a.maxSizeInBytes > b.maxSizeInBytes) a else b
        case (_: AnyType, a) => a
        case (b, _: AnyType) => b
        case (a: DateTimeType, _: DurationType) => a
        case (_: DurationType, b: DateTimeType) => b
        case (a: StringType, _) => a
        case (_, b: StringType) => b
        case (a, b) if a.isNumeric && b.isNumeric =>
          if ((a.isFloatingPoint || b.isFloatingPoint) && !(a.isFloatingPoint && b.isFloatingPoint)) if (a.isFloatingPoint) a else b
          else if (a.maxSizeInBytes > b.maxSizeInBytes) a else b
        case (a, b) => die(s"${a.toSQL} and ${b.toSQL} are incompatible types")
      }
    }
  }

  def resolveType(types: List[DataType]): DataType = resolveType(types: _*)

}

object Inferences extends Inferences {

  final implicit class InstructionTyping(val instruction: Instruction) extends AnyVal {

    @inline def returnType: DataType = instruction match {
      case q: LollypopNative => q.returnType
      case i => Inferences.inferType(i)
    }

  }

}