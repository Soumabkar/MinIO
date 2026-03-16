val m = ETLMetrics(spark)

val cleaned = rawDf.filter { row =>
  val valid = row.getAs[String]("event_date") != null
  if (!valid) m.rowsRejected.add(1)
  else        m.rowsRead.add(1)
  valid
}

// après écriture :
m.rowsWritten.add(resultDf.count())
m.report()