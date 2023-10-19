package com.qwery.language.models

/**
 * Represents a View definition
 * @param query       the given [[Queryable query]]
 * @example
 * {{{
 *   create view OilAndGas as
 *   select Symbol, Name, Sector, Industry, `Summary Quote`
 *   from Customers
 *   where Industry = 'Oil/Gas Transmission'
 * }}}
 */
case class View(query: Queryable) extends Queryable {
  override def toSQL: String = query.toSQL
}
