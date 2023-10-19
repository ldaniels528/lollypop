package com.qwery.runtime.instructions.expressions.aggregation

import com.qwery.runtime.instructions.functions.InternalFunctionCall

/**
 * Represents an aggregate function call
 */
trait AggregateFunctionCall extends InternalFunctionCall with AggregateExpression