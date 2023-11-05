package com.lollypop.language

import com.lollypop.language.SetFieldValues.SetFieldValuesTemplateTag
import com.lollypop.language.TemplateProcessor.tags.TemplateTag
import com.lollypop.language.models.{Instruction, NamedExpression}
import com.lollypop.runtime.instructions.queryables.TableVariableRef

trait SetFieldValues { self: LanguageParser =>

  // add custom tag to set field values (e.g. "%U:values" => "set name = 'Larry', age = 21")
  TemplateProcessor.addTag("U", SetFieldValuesTemplateTag)

}

object SetFieldValues {

  def nextFieldAssignment(stream: TokenStream)(implicit compiler: SQLCompiler): List[(String, Instruction)] = {
    var assignments: List[(String, Instruction)] = Nil
    do {
      val assignment = for {
        ref <- nextIdentifier(stream)
        name = ref.name
        _ = stream.expect("=")
        expr <- ref match {
          case _: TableVariableRef => compiler.nextOpCode(stream)
          case _ => compiler.nextExpression(stream)
        }
      } yield name -> expr
      assignments = assignment.toList ::: assignments
    } while (stream nextIf ",")
    assignments.reverse
  }

  private def nextIdentifier(stream: TokenStream)(implicit compiler: SQLCompiler): Option[NamedExpression] = {
    stream match {
      case ts if ts is "@" => compiler.nextVariableReference(ts)
      case ts if ts is "$" => compiler.nextVariableReference(ts)
      case ts => compiler.nextField(ts)
    }
  }

  case class SetFieldValuesTemplateTag(name: String) extends TemplateTag {
    override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
      SQLTemplateParams(assignments = Map(name -> nextFieldAssignment(stream)))
    }

    override def toCode: String = s"%U:$name"
  }

}