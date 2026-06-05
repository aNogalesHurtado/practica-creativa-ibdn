#!/bin/bash
POD_IP=$(hostname -i)
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --class es.upm.dit.ging.predictor.MakePrediction \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.mongodb:mongodb-driver-sync:5.1.4,org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=admin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.sql.streaming.kafka.useDeprecatedOffsetFetching=false \
  --conf spark.executor.extraJavaOptions="--add-opens java.base/java.nio=ALL-UNNAMED" \
  --conf spark.driver.extraJavaOptions="--add-opens java.base/java.nio=ALL-UNNAMED" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.host=$POD_IP \
  /app/flight_prediction.jar
