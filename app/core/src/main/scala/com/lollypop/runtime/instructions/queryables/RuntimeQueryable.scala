package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.models.{CodeBlock, Instruction, Queryable}
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.RuntimeInstruction
import com.lollypop.runtime.instructions.invocables.{IF, Return}
import com.lollypop.runtime.{DataObject, DatabaseObjectRef, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import scala.language.postfixOps

/**
 * Represents a run-time Queryable
 */
trait RuntimeQueryable extends Queryable with RuntimeInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection)

}

object RuntimeQueryable {

  /**
   * Database Object References Retrieval
   * @param instruction the host [[Instruction instruction]]
   */
  def getQueryReferences(instruction: Instruction): List[DatabaseObjectRef] = {

    def recurse(op: Instruction): List[DatabaseObjectRef] = op match {
      case CodeBlock(ops) => ops.flatMap(recurse)
      case d: DataObject => List(d.ns)
      case d: DatabaseObjectRef => List(d)
      case Describe(source) => recurse(source)
      case From(a) => recurse(a)
      case HashTag(source, _) => recurse(source)
      case IF(_, a, b_?) => (a :: b_?.toList).flatMap(recurse)
      case Intersect(a, b) => recurse(a) ::: recurse(b)
      case ProcedureCall(ref, _) => List(ref)
      case Return(value) => value.toList.flatMap(recurse)
      case s: Select => s.from.toList.flatMap(recurse)
      case Subtraction(a, b) => recurse(a) ::: recurse(b)
      case Union(a, b) => recurse(a) ::: recurse(b)
      case UnionDistinct(a, b) => recurse(a) ::: recurse(b)
      case _ => Nil
    }

    recurse(instruction).distinct
  }

  /**
   * Database Object Reference Detection
   * @param instruction the host [[Instruction instruction]]
   */
  final implicit class DatabaseObjectRefDetection(val instruction: Instruction) extends AnyVal {

    def detectRef: Option[DatabaseObjectRef] = {

      def recurse(op: Instruction): Option[DatabaseObjectRef] = op match {
        case CodeBlock(ops) => ops.flatMap(recurse).lastOption
        case d: DataObject => Option(d.ns)
        case d: DatabaseObjectRef => Some(d)
        case Describe(source) => recurse(source)
        case From(source) => recurse(source)
        case HashTag(source, _) => recurse(source)
        case IF(_, a, b_?) => recurse(a) ?? b_?.flatMap(recurse)
        case Intersect(a, b) => recurse(a) ?? recurse(b)
        case ProcedureCall(ref, _) => Option(ref)
        case Return(value) => value.flatMap(recurse)
        case s: Select => s.from.flatMap(recurse)
        case Subtraction(a, b) => recurse(a) ?? recurse(b)
        case Union(a, b) => recurse(a) ?? recurse(b)
        case UnionDistinct(a, b) => recurse(a) ?? recurse(b)
        case _ => None
      }

      recurse(instruction)
    }
  }

}