file:///D:/training/editions_eni/MinIO/Provisions-Files/project/scala/etl-project/src/main/scala/metrics/sampleInETL.scala
empty definition using pc, found symbol in pc: 
semanticdb not found
empty definition using fallback
non-local guesses:
	 -ETLMetrics.
	 -ETLMetrics#
	 -ETLMetrics().
	 -scala/Predef.ETLMetrics.
	 -scala/Predef.ETLMetrics#
	 -scala/Predef.ETLMetrics().
offset: 10
uri: file:///D:/training/editions_eni/MinIO/Provisions-Files/project/scala/etl-project/src/main/scala/metrics/sampleInETL.scala
text:
```scala
val m = ET@@LMetrics(spark)

val cleaned = rawDf.filter { row =>
  val valid = row.getAs[String]("event_date") != null
  if (!valid) m.rowsRejected.add(1)
  else        m.rowsRead.add(1)
  valid
}

// après écriture :
m.rowsWritten.add(resultDf.count())
m.report()
```


#### Short summary: 

empty definition using pc, found symbol in pc: 