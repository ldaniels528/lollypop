package com.qwery.language

import com.qwery.language.models._
import com.qwery.runtime.DatabaseObjectRef
import com.qwery.runtime.instructions.expressions.Dictionary

/**
 * Represents the extracted SQL template properties
 * @param assignments     the named collection of key-value pairs (e.g. "key = 'Hello', value = 123")
 * @param atoms           the named collection of [[Atom identifiers]] (e.g. "from")
 * @param atomLists       the named collection of [[Atom identifier]] lists (e.g. "symbol, exchange")
 * @param parameters      the named collection of [[Parameter parameters]]
 * @param expressionLists the named collection of key-value pair assignments (e.g. "X" -> "1+(2*x)")
 * @param fieldLists      the named collection of field references (e.g. "insert into (symbol, exchange, lastSale)")
 * @param indicators      the named collection of indicator references (e.g. "if not exists")
 * @param instructions    the named collection of [[Instruction instructions]]
 * @param keywords        the named collection of key words
 * @param locations       the named collection of [[DatabaseObjectRef locations]]
 * @param orderedFields   the named collection of ordered fields (e.g. "order by symbol")
 * @param properties      the named collection of properties
 * @param repeatedSets    the named collection of repeated sequences (e.g. "values ('123', '456') values ('789', '012')")
 * @param types           the named collection of [[ColumnType types]]
 */
case class SQLTemplateParams(assignments: Map[String, List[(String, Instruction)]] = Map.empty,
                             atoms: Map[String, Atom] = Map.empty,
                             atomLists: Map[String, List[Atom]] = Map.empty,
                             parameters: Map[String, List[ParameterLike]] = Map.empty,
                             expressionLists: Map[String, List[Expression]] = Map.empty,
                             fieldLists: Map[String, List[FieldRef]] = Map.empty,
                             indicators: Map[String, Boolean] = Map.empty,
                             instructions: Map[String, Instruction] = Map.empty,
                             keywords: Set[String] = Set.empty,
                             locations: Map[String, DatabaseObjectRef] = Map.empty,
                             mappings: Map[String, Map[Expression, Expression]] = Map.empty,
                             orderedFields: Map[String, List[OrderColumn]] = Map.empty,
                             properties: Map[String, Map[String, String]] = Map.empty,
                             repeatedSets: Map[String, List[SQLTemplateParams]] = Map.empty,
                             types: Map[String, ColumnType] = Map.empty) {

  def +(that: SQLTemplateParams): SQLTemplateParams = {
    this.copy(
      assignments = this.assignments ++ that.assignments,
      atoms = this.atoms ++ that.atoms,
      atomLists = this.atomLists ++ that.atomLists,
      parameters = this.parameters ++ that.parameters,
      expressionLists = this.expressionLists ++ that.expressionLists,
      fieldLists = this.fieldLists ++ that.fieldLists,
      indicators = this.indicators ++ that.indicators,
      instructions = this.instructions ++ that.instructions,
      keywords = this.keywords ++ that.keywords,
      locations = this.locations ++ that.locations,
      mappings = this.mappings ++ that.mappings,
      orderedFields = this.orderedFields ++ that.orderedFields,
      properties = this.properties ++ that.properties,
      repeatedSets = this.repeatedSets ++ that.repeatedSets,
      types = this.types ++ that.types)
  }

  def all: Map[String, Any] = {
    Seq(assignments, atoms, atomLists, parameters, expressionLists, fieldLists, indicators,
      instructions, locations, mappings, orderedFields, properties, repeatedSets, types,
      if (keywords.nonEmpty) Map("keywords" -> keywords) else Map.empty[String, Any]).reduce(_ ++ _)
  }

  def consumables: Map[String, Any] = {
    Seq(assignments, atoms, atomLists, parameters, expressionLists, fieldLists, indicators,
      instructions, locations, mappings, orderedFields, properties, repeatedSets, types).reduce(_ ++ _)
  }

  def conditions: Map[String, Condition] = instructions.collect { case (k, v: Condition) => k -> v }

  def dictionaries: Map[String, Dictionary] = instructions.collect { case (k, v: Dictionary) => k -> v }

  def expressions: Map[String, Expression] = instructions.collect { case (k, v: Expression) => k -> v }

  def queryables: Map[String, Queryable] = instructions.collect { case (k, v: Queryable) => k -> v }

  /**
   * Indicates whether all of the template mappings are empty
   * @return true, if all of the template mappings are empty
   */
  def isEmpty: Boolean = !nonEmpty

  /**
   * Indicates whether at least one of the template mappings is not empty
   * @return true, if at least one of the template mappings is not empty
   */
  def nonEmpty: Boolean = Seq(assignments, atoms, atomLists, parameters, expressionLists, fieldLists, indicators,
    instructions, keywords, locations, mappings, orderedFields, properties, repeatedSets, types).exists(_.nonEmpty)

  override def toString: String = s"${this.getClass.getSimpleName}(${all.toString()})"

}

/**
 * SQLTemplate Params Companion
 * @author lawrence.daniels@gmail.com
 */
object SQLTemplateParams {

  /**
   * Creates a new SQL template parameters instance pre-populated with the tokens parsed via the given template
   * @param ts       the given [[TokenStream token stream]]
   * @param template the given template (e.g. "after %e:delay %N:command")
   * @return the [[SQLTemplateParams template parameters]]
   */
  def apply(ts: TokenStream, template: String)(implicit compiler: SQLCompiler): SQLTemplateParams = Template(template).process(ts)

  final implicit class MappedParameters[T](val mapped: Map[String, T]) extends AnyVal {
    def is(name: String, f: T => Boolean): Boolean = mapped.get(name).exists(f)
  }

}
