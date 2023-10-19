package com.qwery.runtime

import com.qwery.language.dieIllegalObjectRef
import com.qwery.language.models.{@@@, Queryable}
import com.qwery.runtime.instructions.RuntimeInstruction
import com.qwery.runtime.instructions.queryables.TableVariableRef
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.IOCost

/**
 * Represents a reference to a database object
 */
trait DatabaseObjectRef extends Queryable with RuntimeInstruction {

  /**
   * @return the name of the referenced object
   */
  def name: String

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope, IOCost.empty, scope.getRowCollection(this))
  }

  /**
   * Indicates whether a sub-table is being referenced (e.g. "stocks#symbol")
   * @return true, if sub-table is being referenced
   */
  def isSubTable: Boolean = false

  override def toString: String = toSQL

}

/**
 * Data Object Reference Companion
 */
object DatabaseObjectRef {

  /**
   * Parses a database object path
   * @param uri the database object path (e.g. "businesses.currencies.fiat", "nasdaq.stocks" or "stocks#symbol")
   * @return a new [[DatabaseObjectRef object reference]]
   */
  def apply(uri: String): DatabaseObjectRef = {
    @inline
    def parse(path: String): DatabaseObjectRef = path.split('.') match {
      case Array(databaseName, schemaName, name) => DatabaseObjectNS(databaseName, schemaName, name)
      case Array(schemaName, name) => Unrealized(databaseName = None, schemaName = Some(schemaName), name = name)
      case Array(name) if name.startsWith("@@") => @@@(name.drop(2))
      case Array(name) if name.startsWith("@") => @@@(name.drop(1))
      case Array(name) => Unrealized(databaseName = None, schemaName = None, name = name)
      case _ => dieIllegalObjectRef(path)
    }

    // parse the URI (e.g. "businesses.currencies.fiat", "nasdaq.stocks" or "stocks#symbol")
    uri.split('#') match {
      case Array(basePath, name) =>
        parse(basePath) match {
          case ns: DatabaseObjectNS => ns.copy(columnName = Some(name))
          case base: DatabaseObjectRef => InnerTable(base, name)
        }
      case Array(path) => parse(path)
      case _ => dieIllegalObjectRef(uri)
    }
  }

  /**
   * Creates a new sub-table reference
   * @param base the [[DatabaseObjectRef base table reference]]
   * @param name the name of the sub-table
   * @return the [[DatabaseObjectRef sub-table reference]]
   */
  def apply(base: DatabaseObjectRef, name: String): DatabaseObjectRef = {
    base match {
      case ns: DatabaseObjectNS => ns.copy(columnName = Some(name))
      case base => InnerTable(base, name)
    }
  }

  /**
   * Creates a new unrealized object reference
   * @param schemaName the name of the schema
   * @param name       the name of the database object
   * @return the [[DatabaseObjectRef.Unrealized unrealized object reference]]
   */
  def apply(schemaName: String, name: String): DatabaseObjectRef.Unrealized = {
    Unrealized(databaseName = None, schemaName = Option(schemaName), name = name)
  }

  /**
   * Creates a new realized object reference
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param name         the name of the database object
   * @return the [[DatabaseObjectNS realized object reference]]
   */
  def apply(databaseName: String, schemaName: String, name: String): DatabaseObjectNS = {
    DatabaseObjectNS(databaseName, schemaName, name)
  }

  /**
   * Creates a new inner table object reference
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param name         the name of the database object
   * @param columnName   the inner table column name
   * @return the [[DatabaseObjectNS inner table object reference]]
   */
  def apply(databaseName: String, schemaName: String, name: String, columnName: String): DatabaseObjectNS = {
    DatabaseObjectNS(databaseName, schemaName, name, columnName = Some(columnName))
  }

  def unapply(ref: DatabaseObjectRef): Option[(Option[String], Option[String], String, Option[String])] = {
    ref match {
      case it: InnerTable =>
        it.base match {
          case ns: DatabaseObjectNS => Some((Some(ns.databaseName), Some(ns.schemaName), ns.name, Some(it.name)))
          case un: Unrealized => Some((un.databaseName, un.schemaName, un.name, Some(it.name)))
          case _ => None
        }
      case ns: DatabaseObjectNS => Some((Some(ns.databaseName), Some(ns.schemaName), ns.name, ns.columnName))
      case un: Unrealized => Some((un.databaseName, un.schemaName, un.name, None))
      case _ => None
    }
  }

  /**
   * Represents a reference to an inner-table
   * @param base a [[DatabaseObjectRef reference]] to the base table or object
   * @param name the name of the database object
   */
  case class InnerTable(base: DatabaseObjectRef, name: String) extends DatabaseObjectRef {
    override def isSubTable: Boolean = true

    override def toSQL: String = s"$base#$name"
  }

  /**
   * Represents a reference to an sub-table reference
   */
  object SubTable {
    def unapply(ref: DatabaseObjectRef): Option[(DatabaseObjectRef, String)] = {
      ref match {
        case it: InnerTable => Some((it.base, it.name))
        case ns: DatabaseObjectNS => ns.columnName.map(name => (ns.copy(columnName = None), name))
        case _ => None
      }
    }
  }

  /**
   * Represents an "unrealized" reference to a database object
   * @param databaseName the optional name of the database
   * @param schemaName   the optional name of the schema
   * @param name         the name of the database object
   */
  case class Unrealized(databaseName: Option[String] = None, schemaName: Option[String] = None, name: String) extends DatabaseObjectRef {
    override def toSQL: String = (databaseName.toList ::: schemaName.toList ::: name :: Nil).mkString(".")
  }

  /**
   * DatabaseObjectRef Realization
   * @param ref the [[DatabaseObjectRef reference]]
   */
  final implicit class DatabaseObjectRefRealization(val ref: DatabaseObjectRef) extends AnyVal {

    @inline
    def toNS(implicit scope: Scope): DatabaseObjectNS = ref match {
      case ns: DatabaseObjectNS => ns
      case it: DatabaseObjectRef.InnerTable => it.base.toNS.copy(columnName = Some(it.name))
      case un: DatabaseObjectRef.Unrealized =>
        DatabaseObjectNS(
          databaseName = un.databaseName ?? scope.getDatabase || DEFAULT_DATABASE,
          schemaName = un.schemaName ?? scope.getSchema || DEFAULT_SCHEMA,
          name = un.name)
      case tv: TableVariableRef => DatabaseObjectNS(scope.getDatabase || DEFAULT_DATABASE, scope.getSchema || DEFAULT_SCHEMA, tv.toSQL)
      case x => dieIllegalObjectRef(x.toSQL)
    }

    @inline
    def realize(implicit scope: Scope): DatabaseObjectRef = ref match {
      case ns: DatabaseObjectNS => ns
      case it: DatabaseObjectRef.InnerTable => it.toNS
      case un@DatabaseObjectRef.Unrealized(None, None, name) => scope.getTableVariable(name) getOrElse un.toNS
      case un: DatabaseObjectRef.Unrealized => un.toNS
      case tv: TableVariableRef => tv
      case x => dieIllegalObjectRef(x.toSQL)
    }
  }

}