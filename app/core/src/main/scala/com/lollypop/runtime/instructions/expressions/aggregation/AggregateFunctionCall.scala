package com.lollypop.runtime.instructions.expressions.aggregation

import com.lollypop.runtime.instructions.functions.InternalFunctionCall

/**
 * Represents an aggregate function call
 */
trait AggregateFunctionCall extends InternalFunctionCall with AggregateExpression