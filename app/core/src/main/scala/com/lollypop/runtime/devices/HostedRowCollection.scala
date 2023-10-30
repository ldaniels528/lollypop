package com.lollypop.runtime.devices

trait HostedRowCollection extends AbstractRowCollection {

  /**
   * @return the [[RowCollection source]] of the data
   */
  def host: RowCollection

  override def out: RowCollection = host

  override def toString: String = s"${getClass.getSimpleName}($host)"

}

object HostedRowCollection {
  def unapply(hrc: HostedRowCollection): Option[RowCollection] = Some(hrc.host)
}