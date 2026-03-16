package metrics

import org.slf4j.LoggerFactory

object TimedStage {
  private val log = LoggerFactory.getLogger(getClass)

  def apply[T](name: String)(block: => T): T = {
    val start = System.currentTimeMillis()
    log.info(s"[START] $name")
    val result = block
    val elapsed = System.currentTimeMillis() - start
    log.info(s"[END]   $name — ${elapsed}ms")
    result
  }
}