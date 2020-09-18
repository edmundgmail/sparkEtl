package com.genware.etl.common

import org.apache.log4j.Logger

trait AppLogger {
  self =>
  val appLogger = Logger.getLogger(self.getClass)
}
