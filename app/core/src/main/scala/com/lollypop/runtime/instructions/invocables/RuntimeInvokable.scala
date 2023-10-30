package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.models.Invokable
import com.lollypop.runtime.instructions.RuntimeInstruction

/**
 * Represents a run-time Invokable
 */
trait RuntimeInvokable extends Invokable with RuntimeInstruction
