package com.qwery.runtime.plastics

import com.qwery.runtime.plastics.RuntimeClass.resolveClass

import scala.annotation.tailrec

object Tuples {

  /**
   * Creates and populates a new array via reflection
   * @param values the source values
   * @return the new array
   */
  def seqToArray(values: Seq[Any]): Array[_] = {
    val _class = resolveClass(values.flatMap(Option.apply).map(_.getClass), isNullable = values.contains(null))
    val array = java.lang.reflect.Array.newInstance(_class, values.length)
    values.zipWithIndex foreach { case (value, index) => java.lang.reflect.Array.set(array, index, value) }
    array.asInstanceOf[Array[_]]
  }

  @tailrec
  def seqToTuple(value: Any): Option[Any] = {
    import com.qwery.util.OptionHelper.implicits.risky._
    value match {
      case array: Array[_] => seqToTuple(array.toSeq)
      case Seq(a, b) => (a, b)
      case Seq(a, b, c) => (a, b, c)
      case Seq(a, b, c, d) => (a, b, c, d)
      case Seq(a, b, c, d, e) => (a, b, c, d, e)
      case Seq(a, b, c, d, e, f) => (a, b, c, d, e, f)
      case Seq(a, b, c, d, e, f, g) => (a, b, c, d, e, f, g)
      case Seq(a, b, c, d, e, f, g, h) => (a, b, c, d, e, f, g, h)
      case Seq(a, b, c, d, e, f, g, h, i) => (a, b, c, d, e, f, g, h, i)
      case Seq(a, b, c, d, e, f, g, h, i, j) => (a, b, c, d, e, f, g, h, i, j)
      case Seq(a, b, c, d, e, f, g, h, i, j, k) => (a, b, c, d, e, f, g, h, i, j, k)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l) => (a, b, c, d, e, f, g, h, i, j, k, l)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m) => (a, b, c, d, e, f, g, h, i, j, k, l, m)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
      case _ => None
    }
  }

  def tupleToSeq(value: Any): Option[List[Any]] = {
    import com.qwery.util.OptionHelper.implicits.risky._
    value match {
      case (a, b) => List(a, b)
      case (a, b, c) => List(a, b, c)
      case (a, b, c, d) => List(a, b, c, d)
      case (a, b, c, d, e) => List(a, b, c, d, e)
      case (a, b, c, d, e, f) => List(a, b, c, d, e, f)
      case (a, b, c, d, e, f, g) => List(a, b, c, d, e, f, g)
      case (a, b, c, d, e, f, g, h) => List(a, b, c, d, e, f, g, h)
      case (a, b, c, d, e, f, g, h, i) => List(a, b, c, d, e, f, g, h, i)
      case (a, b, c, d, e, f, g, h, i, j) => List(a, b, c, d, e, f, g, h, i, j)
      case (a, b, c, d, e, f, g, h, i, j, k) => List(a, b, c, d, e, f, g, h, i, j, k)
      case (a, b, c, d, e, f, g, h, i, j, k, l) => List(a, b, c, d, e, f, g, h, i, j, k, l)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m) => List(a, b, c, d, e, f, g, h, i, j, k, l, m)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
      case _ => None
    }
  }

}
