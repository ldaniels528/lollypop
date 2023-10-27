package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.HelpDoc.{CATEGORY_TRANSFORMATION, PARADIGM_DECLARATIVE}
import com.qwery.language._
import com.qwery.language.models.ColumnType
import com.qwery.runtime.DatabaseManagementSystem.createUserType
import com.qwery.runtime.{DatabaseObjectRef, Scope}
import qwery.io.IOCost

import scala.collection.mutable

/**
 * create type statement
 * @param ref         the [[DatabaseObjectRef persistent type reference]]
 * @param userType    the given [[ColumnType user type]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @example {{{ create type if not exists clob8K as CLOB(8192) }}}
 * @example {{{ create type mood as ENUM (sad, okay, happy) }}}
 * @example {{{ create type priceHistory as table (price Double, updatedTime DATE) }}}
 * @author lawrence.daniels@gmail.com
 */
case class CreateType(ref: DatabaseObjectRef, userType: ColumnType, ifNotExists: Boolean)
  extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val cost = createUserType(ref.toNS, userType, ifNotExists)
    (scope, cost, cost)
  }

  override def toSQL: String = {
    val sb = new mutable.StringBuilder("create type ")
    if (ifNotExists) sb.append("if not exists ")
    sb.append(s"${ref.toSQL} := ${userType.toSQL}")
    sb.toString()
  }

}

object CreateType extends ModifiableParser with IfNotExists {
  val template: String = "create type ?%IFNE:exists %L:name %C(_|:=|as) %T:type"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "create type",
    category = CATEGORY_TRANSFORMATION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Creates a database type",
    example = "create type mood := Enum (sad, okay, happy)"
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): CreateType = {
    val params = SQLTemplateParams(ts, template)
    CreateType(ref = params.locations("name"), userType = params.types("type"), ifNotExists = params.indicators.get("exists").contains(true))
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "create type"

}
