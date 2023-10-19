package com.qwery.runtime

import com.qwery.language.models.{ColumnType, Parameter}
import com.qwery.runtime.DatabaseObjectConfig._
import com.qwery.runtime.devices.TableColumn
import qwery.lang.Pointer

/**
 * Represents a database object configuration
 * @param columns                  the database object's [[TableColumn columns]]
 * @param description              the database object's description
 * @param externalFunction         the optional [[ExternalFunctionConfig external function configuration]]
 * @param externalTable            the optional [[ExternalTableConfig external table configuration]]
 * @param functionConfig           the optional [[FunctionConfig native function configuration]]
 * @param indices                  the optional collection of [[HashIndexConfig index configurations]]
 * @param macroConfig              the optional [[MacroConfig macro configuration]]
 * @param persistentVariableConfig the collection of [[PersistentVariableConfig persistent variable configurations]]
 * @param procedure                the optional [[ProcedureConfig procedure configuration]]
 * @param userDefinedType          the optional [[UserDefinedTypeConfig user type configuration]]
 * @param virtualTable             the optional [[VirtualTableConfig virtual table configuration]]
 */
case class DatabaseObjectConfig(columns: Seq[TableColumn],
                                description: Option[String] = None,
                                externalFunction: Option[ExternalFunctionConfig] = None,
                                externalTable: Option[ExternalTableConfig] = None,
                                functionConfig: Option[FunctionConfig] = None,
                                indices: List[HashIndexConfig] = Nil,
                                macroConfig: Option[MacroConfig] = None,
                                partitions: List[String] = Nil,
                                persistentVariableConfig: Option[PersistentVariableConfig] = None,
                                procedure: Option[ProcedureConfig] = None,
                                userDefinedType: Option[UserDefinedTypeConfig] = None,
                                virtualTable: Option[VirtualTableConfig] = None)

/**
 * Database Object Config Companion
 */
object DatabaseObjectConfig {

  case class ExternalFunctionConfig(`class`: String, jar: Option[String])

  case class ExternalTableConfig(format: Option[String],
                                 location: Option[String],
                                 fieldDelimiter: Option[String] = None,
                                 lineTerminator: Option[String] = None,
                                 headers: Option[Boolean] = None,
                                 nullValues: List[String] = Nil)

  case class FunctionConfig(sql: String)

  case class HashIndexConfig(indexedColumnName: String, isUnique: Boolean)

  case class MacroConfig(sql: String, template: String)

  case class PersistentVariableConfig(ref: DatabaseObjectRef, `type`: ColumnType, pointer: Pointer)

  case class ProcedureConfig(parameters: Seq[Parameter], sql: String)

  case class UserDefinedTypeConfig(sql: String)

  case class VirtualTableConfig(sql: String)

  //////////////////////////////////////////////////////////////////////////////////////
  //  IMPLICIT DEFINITIONS
  //////////////////////////////////////////////////////////////////////////////////////

  object implicits {

    /**
     * Rich Database object Config
     * @param config the host [[DatabaseObjectConfig config]]
     */
    final implicit class RichDatabaseEntityConfig(val config: DatabaseObjectConfig) extends AnyVal {
      @inline def isExternalTable: Boolean = config.externalTable.nonEmpty

      @inline def isFunction: Boolean = config.functionConfig.nonEmpty

      @inline def isMACRO: Boolean = config.macroConfig.nonEmpty

      @inline def isProcedure: Boolean = config.procedure.nonEmpty

      @inline def isUserType: Boolean = config.userDefinedType.nonEmpty

      @inline def isVirtualTable: Boolean = config.virtualTable.nonEmpty
    }

  }

}