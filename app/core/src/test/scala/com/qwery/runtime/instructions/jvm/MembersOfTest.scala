package com.qwery.runtime.instructions.jvm

import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class MembersOfTest extends AnyFunSpec {

  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[MembersOf].getSimpleName) {

    it("should retrieve constructors, fields and methods of a class or object") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|from (membersOf(classOf("java.sql.Ref")))
           |""".stripMargin
      )
      assert(device.toMapGraph == List(
        Map("modifiers" -> "public abstract", "member" -> "getBaseTypeName()", "returnType" -> "String", "memberType" -> "Method"),
        Map("modifiers" -> "public abstract", "member" -> "getObject()", "returnType" -> "Object", "memberType" -> "Method"),
        Map("modifiers" -> "public abstract", "member" -> "getObject(arg0: Map)", "returnType" -> "Object", "memberType" -> "Method"),
        Map("modifiers" -> "public abstract", "member" -> "setObject(arg0: Object)", "returnType" -> "void", "memberType" -> "Method"),
        Map("modifiers" -> "public", "member" -> "toTable()", "returnType" -> "RowCollection", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "toPrettyString()", "returnType" -> "String", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "toJsonPretty()", "returnType" -> "String", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "toJsonString()", "returnType" -> "String", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "toJson()", "returnType" -> "String", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "serialize()", "returnType" -> "byte[]", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "isUUID()", "returnType" -> "boolean", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "isTable()", "returnType" -> "boolean", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "isString()", "returnType" -> "boolean", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "isNumber()", "returnType" -> "boolean", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "isISO8601()", "returnType" -> "boolean", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "isFunction()", "returnType" -> "boolean", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "isDateTime()", "returnType" -> "boolean", "memberType" -> "virtual method"),
        Map("modifiers" -> "public", "member" -> "isArray()", "returnType" -> "boolean", "memberType" -> "virtual method")
      ))
    }

  }

}
