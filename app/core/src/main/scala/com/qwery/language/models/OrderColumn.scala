package com.qwery.language.models

/**
 * Represents an Ordered Column
 * @param name        the name of the column
 * @param isAscending indicates whether the column is ascending (or conversely descending)
 */
case class OrderColumn(name: String, isAscending: Boolean) extends Instruction {
  override def toSQL: String = s"$name ${if (isAscending) "asc" else "desc"}"
}
