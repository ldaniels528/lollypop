package com.qwery.language

import com.qwery.runtime.instructions.expressions.ScalarVariableRef
import com.qwery.runtime.instructions.queryables.TableVariableRef

package object models {
  
  /**
   * Shortcut for creating a scalar variable reference
   * @param name the name of the scalar variable
   * @return a new [[ScalarVariableRef scalar variable reference]]
   */
  def @@(name: String): ScalarVariableRef = ScalarVariableRef(name)

  /**
   * Shortcut for creating a table variable reference
   * @param name the name of the table variable
   * @return a new [[TableVariableRef table variable reference]]
   */
  def @@@(name: String): TableVariableRef = TableVariableRef(name)

}
