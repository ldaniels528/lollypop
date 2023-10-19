package com.qwery.runtime.datatypes

import com.qwery.language.models.Functional
import qwery.io.{Decoder, Encoder}

/**
 * Represents a Functional Type
 * @tparam T the run-time type this functional type produces
 */
trait FunctionalType[T] extends DataType with Functional
  with ConstructorSupport[T]
  with Decoder[T]
  with Encoder {

  override def construct(args: Seq[Any]): T = {
    args match {
      case Seq(value) => convert(value).asInstanceOf[T]
      case s => dieArgumentMismatch(args = s.size, minArgs = 1, maxArgs = 1)
    }
  }

}

