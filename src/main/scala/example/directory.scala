//spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 ./kafkaspark_2.11-0.1.0-SNAPSHOT.jar

package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField}
import java.util.{Collections, Properties}
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import java.util.function.Consumer





object KafkaConsumerSubscribeApp {
  def main(args: Array[String]): Unit = {

    val consumer = new MyConsumer
    val producer = new MyProducer
    val test = new Test12

  

   


    //Kafka Source
      println("Please make a Selection:")
      println("[1] Console \n[2] Add to CSV \n[3] Producer\n[4] Static Consumer\n" +
        "[5] test")
      var choice = scala.io.StdIn.readLine()
      choice match{
        case "exit" => //Exit to directory
        case "1" => consumer.console()
        case "2" => consumer.csv()
        case "3" => producer.produce()
        case "4" => consumer.staticConsumer()
        case "5" => test.test()
      }//End of 'choice match'

  }//End of Main
}//End of Object
