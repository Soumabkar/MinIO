package metrics

import org.apache.spark.scheduler._
import org.slf4j.LoggerFactory

class MetricsListener extends SparkListener {

  private val log = LoggerFactory.getLogger(getClass)

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val info    = event.stageInfo
    val metrics = info.taskMetrics

    log.info(s"""
      |=== Stage ${info.stageId} — ${info.name} ===
      |  Durée              : ${info.duration.getOrElse(0L)} ms
      |  Nb tâches          : ${info.numTasks}
      |  Bytes lus (S3)     : ${metrics.inputMetrics.bytesRead} bytes
      |  Fichiers lus       : ${metrics.inputMetrics.recordsRead} records
      |  Bytes écrits       : ${metrics.outputMetrics.bytesWritten} bytes
      |  Shuffle read       : ${metrics.shuffleReadMetrics.totalBytesRead} bytes
      |  Shuffle write      : ${metrics.shuffleWriteMetrics.bytesWritten} bytes
      |  Peak executor RAM  : ${metrics.peakExecutionMemory} bytes
      |  GC time            : ${metrics.jvmGCTime} ms
      |  Spill disque       : ${metrics.diskBytesSpilled} bytes
      """.stripMargin)
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit =
    log.info(s"Job ${event.jobId} terminé — résultat: ${event.jobResult}")
}