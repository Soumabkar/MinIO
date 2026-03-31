# Compiler et packager
```bash
sbt assembly
```

# Exporter les variables d'environnement
```bash
export $(grep -v '^#' conf/env | sed 's/[[:space:]]*$//' | xargs)
```


# Soumettre à Spark
```bash
spark-submit \
  --class com.lakehouse.Main \
  --master local[*] \
  target/scala-2.12/lakehouse-pipeline-assembly-1.0.0.jar
```
