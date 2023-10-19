package com.qwery.runtime

import java.net.{URL, URLClassLoader}

/**
 * Dynamic ClassLoader
 */
class DynamicClassLoader(parent: ClassLoader, urls: Array[URL] = Array.empty) extends URLClassLoader(urls, parent) {

  override def addURL(url: URL): Unit = super.addURL(url)

}
