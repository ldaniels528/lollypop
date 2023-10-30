package com.lollypop.language.models

/**
 * Represents an executable procedure
 * @param params the procedure's parameters
 * @param code   the procedure's code
 */
case class Procedure(params: Seq[ParameterLike], code: Instruction) extends TypicalFunction