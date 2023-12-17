package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Inequality

/**
 * Represents a run-time Inequality
 */
trait RuntimeInequality extends Inequality with RuntimeCondition
