package com.qwery.runtime.instructions.functions

import com.qwery.language.models.AllFields.dieArgumentMismatch
import com.qwery.language.models.{ColumnType, Expression}

/**
 * Represents a scalar function call
 */
trait ScalarFunctionCall extends InternalFunctionCall

/**
 * ScalarFunctionCall Companion
 */
object ScalarFunctionCall {

  final implicit class ArgumentExtraction(val args: List[Expression]) extends AnyVal {

    @inline
    def extract1: Expression = args match {
      case expr0 :: Nil => expr0
      case _ => dieArgumentMismatch(args = args.length, minArgs = 1, maxArgs = 1)
    }

    @inline
    def extract1or2: (Expression, Option[Expression]) = args match {
      case expr0 :: Nil => (expr0, None)
      case expr0 :: expr1 :: Nil => (expr0, Option(expr1))
      case _ => dieArgumentMismatch(args = args.length, minArgs = 1, maxArgs = 2)
    }

    @inline
    def extract2: (Expression, Expression) = args match {
      case expr0 :: expr1 :: Nil => (expr0, expr1)
      case _ => dieArgumentMismatch(args = args.length, minArgs = 2, maxArgs = 2)
    }

    @inline
    def extract2EC: (Expression, ColumnType) = args match {
      case List(expr0, expr1: ColumnType) => (expr0, expr1)
      case _ => dieArgumentMismatch(args = args.length, minArgs = 2, maxArgs = 2)
    }

    @inline
    def extract2or3: (Expression, Expression, Option[Expression]) = args match {
      case expr0 :: expr1 :: Nil => (expr0, expr1, None)
      case expr0 :: expr1 :: expr2 :: Nil => (expr0, expr1, Option(expr2))
      case _ => dieArgumentMismatch(args = args.length, minArgs = 2, maxArgs = 3)
    }

    @inline
    def extract3: (Expression, Expression, Expression) = args match {
      case expr0 :: expr1 :: expr2 :: Nil => (expr0, expr1, expr2)
      case _ => dieArgumentMismatch(args = args.length, minArgs = 3, maxArgs = 3)
    }

    @inline
    def extract3or4: (Expression, Expression, Expression, Option[Expression]) = args match {
      case expr0 :: expr1 :: expr2 :: Nil => (expr0, expr1, expr2, None)
      case expr0 :: expr1 :: expr2 :: expr3 :: Nil => (expr0, expr1, expr2, Option(expr3))
      case _ => dieArgumentMismatch(args = args.length, minArgs = 3, maxArgs = 4)
    }

  }


}