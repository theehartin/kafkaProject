package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField}
import java.util.{Collections, Properties}
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class MyConsumer{

  val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("firstProducerProgram")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

  //KafkaSource
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    .option("subscribe", "test_topic")
    .option("startingOffsets", "earliest")
    .load()


  val dfOut = df
    .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
    .select("key","value")



  def console(){
    dfOut.writeStream
      .format("console")
      .start()
      .awaitTermination()
  }//End of console()


  def csv(){
    val dfp = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    .option("subscribe", "test_topic")
    //.option("startingOffsets", "earliest")
    .load()

    val dfOutp = dfp
    .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
    .select("key","value")


     dfOutp.writeStream
      .outputMode("append")
      .format("csv")
      .option("path", "file:///home/maria_dev/Project3/sinkTestBEEP/")
      .option("header", false)
      .option("checkpointLocation", "file:///home/maria_dev/Project3/checkpoints/sinkTestCheckBEEP")
      .start()
      .awaitTermination()  
  }//End of csv()

  def toTopic(){
    val dfp = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    .option("subscribe", "test_topic")
    //.option("startingOffsets", "earliest")
    .load()

    val dfOutp = dfp
    .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
    .select("key","value")


  }



  def staticConsumer(){

    import scala.reflect.io.Directory
    import java.io.File

    val directory = new Directory(new File("file:///home/maria_dev/Project3/checkpoints/staticTest2"))
    directory.deleteRecursively()

    val streamingContext = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    .option("subscribe", "test_topic")
    .option("startingOffsets", "earliest")
    .load()

    val streamingContextOut = streamingContext
    .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
    .select("key","value")


    streamingContextOut.writeStream
    .outputMode("append")
    .format("csv")
    .option("path", "file:///home/maria_dev/Project3/staticTest2")
    .option("header", false)
    .option("checkpointLocation", "file:///home/maria_dev/Project3/checkpoints/staticTest2")
    .start()
    .awaitTermination()

    

    


    


/* 
    import org.apache.kafka.clients.consumer._

    val props: Properties = new Properties()
    props.put("group.id", "test")
    // props.put("bootstrap.servers","localhost:9092")
    props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    props.put(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    props.put(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    //props.put("auto.offset.reset", "earliest")
    val consumer = new KafkaConsumer(props)
    val topics = List("test_topic")
    var condition = true
    try {

      consumer.subscribe(topics.asJava)
      consumer.seekToBeginning(consumer.assignment())

      var i = 0 
      while (condition) {
        val records = consumer.poll(10)
        for (record <- records.asScala) {

          println(
            "Topic: " + record.topic() +
              ",Key: " + record.key() +
              ",Value: " + record.value() +
              ", Offset: " + record.offset() +
              ", Partition: " + record.partition()
          )
          i += 1
        }//End of For
        if(i>1000){
          condition = false
        }
      }//End of While
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }

 */


   /*  
    import org.apache.kafka.clients.consumer._
    import org.apache.kafka.clients.consumer.ConsumerRecords
    import org.apache.kafka.clients.consumer.KafkaConsumer
    import org.apache.kafka.common.TopicPartition
    val props: Properties = new Properties()
    props.put("group.id", "test")
    // props.put("bootstrap.servers","localhost:9092")
    props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    props.put(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    props.put(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    val consumer = new KafkaConsumer(props)

    val partitionCollection = consumer.assignment()

    //val topic = "test_topic"
    //val partition = new TopicPartition(topic,1)
    val topics = List("test_topic")
    //val partition = consumer.partitionsFor("test_topic").partition()
    try {

      //consumer.seekToBeginning(partitionCollection)
      consumer.subscribe(topics.asJava)
      //consumer.seekToBeginning(consumer.assignment())


      //consumer.seekToBeginning(partition)
      //consumer.seekToBeginning(partitionCollection)


      while (true) {
        val records = consumer.poll(10)
        //val records = consumer.count()
        for (record <- records.asScala) {
          println(
            "Topic: " + record.topic() +
              ",Key: " + record.key() +
              ",Value: " + record.value() +
              ", Offset: " + record.offset() +
              ", Partition: " + record.partition()
          )
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }





 */









/* 
    import org.apache.kafka.clients.consumer.KafkaConsumer
    val props: Properties = new Properties()
    props.put("group.id", "test")
    // props.put("bootstrap.servers","localhost:9092")
    props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    props.put(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    props.put(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    //props.put("auto.offset.reset", "earliest")
    val consumer = new KafkaConsumer(props)
    val topics = List("test_topic")
    var condition = true
    try {
      consumer.subscribe(topics.asJava)
      while (condition) {

        val records = consumer.poll(10)
        
        for (record <- records.asScala) {
          println(
            "Topic: " + record.topic() +
              ",Key: " + record.key() +
              ",Value: " + record.value() +
              ", Offset: " + record.offset() +
              ", Partition: " + record.partition()
          )
        }
        //condition = false
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }

 */

  }//End of staticConsumer()


  












/* 
Option 1
  {
    "key" : "Screener"
    "value" : {
      "id" : "68635"
      "first_name" : "Billy"
      "last_name" : "Bob"
    }
  }


Option 2
  {
    "key" : ~some randomly generated key~
    "value" : {
      "field" : "screeners"
      "id" : "68635"
      "first_name" : "Billy"
      "last_name" : "Bob"
    }
  }
 */
    
  






}