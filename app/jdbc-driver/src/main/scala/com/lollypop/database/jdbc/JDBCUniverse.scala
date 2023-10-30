package com.lollypop.database.jdbc

import com.lollypop.language.LollypopUniverse

/**
 * JDBC Universe
 */
object JDBCUniverse  {

  def apply(): LollypopUniverse = LollypopUniverse(escapeCharacter = '"')

}
