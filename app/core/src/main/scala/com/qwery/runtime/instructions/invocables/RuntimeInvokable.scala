package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Invokable
import com.qwery.runtime.instructions.RuntimeInstruction

/**
 * Represents a run-time Invokable
 */
trait RuntimeInvokable extends Invokable with RuntimeInstruction
