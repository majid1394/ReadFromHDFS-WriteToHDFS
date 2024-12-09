import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.net.URI

object WordCountSpark {

  def main(args: Array[String]): Unit = {

 /*  val hdfs = FileSystem.get(new URI("hdfs://master:9000/"), new Configuration())
   val path = new Path("user/temp/4300-0.txt")//ba put anjam shod
   val stream = hdfs.open(path)
   def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))*/

    val inputFile= "C:\\wordCount\\4300-0.txt";
    val outputFile="C:\\dataset1\\output.txt"

    val conf=new SparkConf().setMaster("local").setAppName("My App")
    val sc=new SparkContext(conf)


      val inputRDD =sc.textFile(inputFile)
      println(s"Total Lines: ${inputRDD.count()}=")

      val contentArr =inputRDD.collect()
      println("content:")
      contentArr.foreach(println)

    val words: RDD[String]=inputRDD.flatMap(line=>line.split(" "))
    val count1PerWords: RDD[(String,Int)]=
      words.map(word => (word, 1))

    val counts: RDD[(String,Int)]=count1PerWords.reduceByKey {

      case (counter, nextVal) => counter + nextVal
    }


    FileUtils.deleteQuietly(new File(outputFile))
    counts.saveAsTextFile(outputFile)
    println("Program executed successfully")



    }

}
