package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.{Expression, ParameterLike}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.{AnyType, DataType, StringType, TableType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.devices.{RowCollection, TableColumn}
import com.lollypop.runtime.instructions.expressions.TableExpression
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.lollypop.runtime.instructions.jvm.MembersOf.ParameterLikeToScalaCode
import com.lollypop.runtime.instructions.queryables.RuntimeQueryable
import com.lollypop.runtime.plastics.Plastic.implicits.MethodNameConverter
import com.lollypop.runtime.plastics.RuntimeClass
import com.lollypop.runtime.plastics.RuntimeClass.decodeModifiers
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassConstructorSugar
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import java.lang.reflect.Modifier

/**
 * Retrieves the constructor, fields and methods for a Class or instance
 * @param expression the [[Expression]] which represents a Class or instance
 * @example {{{ membersOf(classOf('java.sql.Date')) }}}
 * @example {{{ membersOf(classOf('java.lang.String')) where memberType is "virtual method" }}}
 */
case class MembersOf(expression: Expression) extends ScalarFunctionCall with RuntimeQueryable with TableExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    // get the class components
    val comp_? = for {
      (_instance, _class) <- Option(expression.execute(scope)._3) map {
        case c: Class[_] => c -> c
        case x => x -> x.getClass
      }
    } yield (_instance, _class, RuntimeClass(_class))

    // include the constructor, fields and methods
    val members = for {
      (_, _, comps) <- comp_?.toList
      constructors = comps.constructors.toList.filterNot(_.getName.contains("__$")).sortBy(_.getName)
      fields = comps.fields.values.toList.filterNot(_.getName.contains("__$")).sortBy(_.getName)
      methods = comps.methods.values.toList.flatMap(_.toList).filterNot(_.getName.contains("__$")).sortBy(_.getName)
      member <- constructors ::: fields ::: methods
    } yield Map(
      "member" -> (member match {
        case e: java.lang.reflect.Executable =>
          e.getName.decodeName + e.getParameters.map(p => s"${p.getName.decodeName}: ${p.getType.getJavaTypeName.decodeName}").mkString("(", ", ", ")")
        case x => x.getName.decodeName
      }),
      "modifiers" -> decodeModifiers(member.getModifiers).mkString(" "),
      "memberType" -> member.getClass.getSimpleName,
      "returnType" -> (member match {
        case f: java.lang.reflect.Field => f.getType.getJavaTypeName
        case m: java.lang.reflect.Method => m.getReturnType.getJavaTypeName
        case x => x.getName
      }).decodeName
    )

    // gather virtual methods
    val virtualMethods = for {
      (_instance, _, _) <- comp_?.toList
      vmc <- RuntimeClass.getVirtualMethods(_instance)
      vmFx = vmc.method
    } yield Map(
      "member" -> (vmFx.name.decodeName + vmFx.params.tail.map(_.toScalaCode).mkString("(", "", ")")),
      "modifiers" -> decodeModifiers(Modifier.PUBLIC).mkString(" "),
      "memberType" -> "virtual method",
      "returnType" -> (vmFx.returnType_?.map(DataType.apply) || AnyType).toJavaType(hasNulls = false).getSimpleName.decodeName
    )

    // generate the table
    implicit val out: RowCollection = createQueryResultTable(returnType.columns)
    val cost = (members ::: virtualMethods) map { member =>
      out.insert(member.toRow)
    } reduce (_ ++ _)
    (scope, cost, out)
  }

  override val returnType: TableType = TableType(columns = Seq(
    TableColumn(name = "modifiers", `type` = StringType),
    TableColumn(name = "member", `type` = StringType),
    TableColumn(name = "returnType", `type` = StringType),
    TableColumn(name = "memberType", `type` = StringType),
  ))

}

object MembersOf extends FunctionCallParserE1(
  name = "membersOf",
  category = CATEGORY_JVM_REFLECTION,
  paradigm = PARADIGM_FUNCTIONAL,
  description = "Returns the members (constructors, fields and methods) of a JVM Class as a Table",
  example = "from membersOf(new `java.util.Date`()) limit 5") {

  final implicit class ParameterLikeToScalaCode(val parameterLike: ParameterLike) extends AnyVal {
    @inline
    def toScalaCode(implicit scope: Scope): String = {
      ((parameterLike.name + ":") :: DataType(parameterLike.`type`).toJavaType(false).getSimpleName ::
        parameterLike.defaultValue.toList.flatMap(e => List("=", e.toSQL))).mkString(" ")
    }
  }

}

