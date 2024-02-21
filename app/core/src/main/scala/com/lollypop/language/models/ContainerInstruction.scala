package com.lollypop.language.models

/**
 * Represents a container instruction
 */
trait ContainerInstruction extends Instruction {

  def code: Instruction

}