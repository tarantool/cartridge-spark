package io.tarantool.spark.connector.util

import org.slf4j.{Logger, LoggerFactory}

/**
  * Provides logging facility via SLF4J
  *
  * @author Alexey Kuzin
  */
private[spark] trait LoggingTrait {

  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var log_ : Logger = null // scalastyle:ignore

  protected def logName: String =
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")

  protected def log: Logger = {
    if (log_ == null) { // scalastyle:ignore
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  protected def logInfo(msg: => String): Unit =
    if (log.isInfoEnabled) log.info(msg)

  protected def logDebug(msg: => String): Unit =
    if (log.isDebugEnabled) log.debug(msg)

  protected def logTrace(msg: => String): Unit =
    if (log.isTraceEnabled) log.trace(msg)

  protected def logWarning(msg: => String): Unit =
    if (log.isWarnEnabled) log.warn(msg)

  protected def logError(msg: => String): Unit =
    if (log.isErrorEnabled) log.error(msg)
}
