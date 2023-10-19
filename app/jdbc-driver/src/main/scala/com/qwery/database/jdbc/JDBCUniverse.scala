package com.qwery.database.jdbc

import com.qwery.language.QweryUniverse

/**
 * JDBC Universe
 */
object JDBCUniverse  {

  def apply(): QweryUniverse = QweryUniverse(escapeCharacter = '"')

}
