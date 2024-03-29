package io.tarantool.spark.connector

import org.slf4j.{Logger, LoggerFactory}

/**
  * Utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
  * logging messages at different levels using methods that only evaluate parameters lazily if the
  * log level is enabled.
  *
  *
  * This is a copy of what Spark Previously held in org.apache.spark.Logging. That class is
  * now private so we will expose similar functionality here.
  */
trait Logging {
  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var _log: Logger = _

  protected def logInfo(msg: => String): Unit =
    if (log.isInfoEnabled) log.info(msg)

  protected def logDebug(msg: => String): Unit =
    if (log.isDebugEnabled) log.debug(msg)

  protected def logTrace(msg: => String): Unit =
    if (log.isTraceEnabled) log.trace(msg)

  protected def logWarning(msg: => String): Unit =
    if (log.isWarnEnabled) log.warn(msg)

  protected def log: Logger = {
    if (_log == null) {
      _log = LoggerFactory.getLogger(logName)
    }
    _log
  }

  protected def logName: String =
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")

  protected def logError(msg: => String): Unit =
    if (log.isErrorEnabled) log.error(msg)

  protected def logInfo(msg: => String, throwable: Throwable): Unit =
    if (log.isInfoEnabled) log.info(msg, throwable)

  protected def logDebug(msg: => String, throwable: Throwable): Unit =
    if (log.isDebugEnabled) log.debug(msg, throwable)

  protected def logTrace(msg: => String, throwable: Throwable): Unit =
    if (log.isTraceEnabled) log.trace(msg, throwable)

  protected def logWarning(msg: => String, throwable: Throwable): Unit =
    if (log.isWarnEnabled) log.warn(msg, throwable)

  protected def logError(msg: => String, throwable: Throwable): Unit =
    if (log.isErrorEnabled) log.error(msg, throwable)

  protected def isTraceEnabled: Boolean =
    log.isTraceEnabled
}
