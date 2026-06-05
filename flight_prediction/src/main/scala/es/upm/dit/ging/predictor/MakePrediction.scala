package es.upm.dit.ging.predictor

import com.mongodb.client.MongoClients
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions.{concat, from_json, lit}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

object MakePrediction {

  def main(args: Array[String]): Unit = {
    println("Fligth predictor starting...")

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    import spark.implicits._

    val base_path=sys.env.getOrElse("MODELS_PATH", "s3a://lakehouse")
    val arrivalBucketizerPath = "%s/models/arrival_bucketizer_2.0.bin".format(base_path)
    val arrivalBucketizer = Bucketizer.load(arrivalBucketizerPath)
    val columns= Seq("Carrier","Origin","Dest","Route")

    val stringIndexerModelPath = columns.map(n=> ("%s/models/string_indexer_model_"
      .format(base_path)+"%s.bin".format(n)).toSeq)
    val stringIndexerModel = stringIndexerModelPath.map{n => StringIndexerModel.load(n.toString)}
    val stringIndexerModels = (columns zip stringIndexerModel).toMap

    val vectorAssemblerPath = "%s/models/numeric_vector_assembler.bin".format(base_path)
    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)

    val randomForestModelPath = "%s/models/spark_random_forest_classifier.flight_delays.5.0.bin".format(base_path)
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
      .option("subscribe", "flight-delay-ml-request")
      .load()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("Origin", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("Dest", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Prediction", DataTypes.StringType)
      .add("Timestamp", DataTypes.TimestampType)
      .add("FlightDate", DataTypes.DateType)
      .add("Carrier", DataTypes.StringType)
      .add("UUID", DataTypes.StringType)
      .add("Distance", DataTypes.DoubleType)
      .add("Carrier_index", DataTypes.DoubleType)
      .add("Origin_index", DataTypes.DoubleType)
      .add("Dest_index", DataTypes.DoubleType)
      .add("Route_index", DataTypes.DoubleType)

    val flightNestedDf = flightJsonDf.select(from_json($"value", struct).as("flight"))

    val flightFlattenedDf = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
      "flight.DepDelay","flight.Timestamp","flight.FlightDate",
      "flight.Carrier","flight.UUID","flight.Distance")

    val predictionRequestsWithRouteMod = flightFlattenedDf.withColumn(
      "Route", concat(flightFlattenedDf("Origin"), lit('-'), flightFlattenedDf("Dest")))

    val flightFlattenedDf2 = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
      "flight.DepDelay","flight.Timestamp","flight.FlightDate",
      "flight.Carrier","flight.UUID","flight.Distance",
      "flight.Carrier_index","flight.Origin_index","flight.Dest_index","flight.Route_index")

    val predictionRequestsWithRouteMod2 = flightFlattenedDf2.withColumn(
      "Route", concat(flightFlattenedDf2("Origin"), lit('-'), flightFlattenedDf2("Dest")))

    val predictionRequestsWithRoute = stringIndexerModel.map(n=>n.transform(predictionRequestsWithRouteMod))

    val vectorizedFeatures = vectorAssembler.setHandleInvalid("keep").transform(predictionRequestsWithRouteMod2)

    val finalVectorizedFeatures = vectorizedFeatures
      .drop("Carrier_index").drop("Origin_index").drop("Dest_index").drop("Route_index")

    val predictions = rfc.transform(finalVectorizedFeatures).drop("Features_vec")

    val finalPredictions = predictions.drop("indices").drop("values").drop("rawPrediction").drop("probability")

// Write to Kafka, Cassandra and MongoDB
    val query = finalPredictions.writeStream
      .outputMode("append")
      .option("checkpointLocation", "/tmp/checkpoint_mongo")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        // 1. Escribir en Kafka
        batchDF.selectExpr("to_json(struct(*)) AS value")
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
          .option("topic", "flight-delay-ml-response")
          .save()

        // 2. Escribir en MongoDB
        val mongoClient = MongoClients.create(s"mongodb://${sys.env.getOrElse("MONGO_HOST", "localhost")}:27017")
        val collection = mongoClient.getDatabase("agile_data_science")
          .getCollection("flight_delay_ml_response")
        batchDF.toJSON.collect().foreach { jsonStr =>
          collection.insertOne(Document.parse(jsonStr))
        }
        mongoClient.close()
      }
      .start()

    val consoleOutput = finalPredictions.writeStream
      .outputMode("append")
      .format("console")
      .start()

    consoleOutput.awaitTermination()
  }
}
