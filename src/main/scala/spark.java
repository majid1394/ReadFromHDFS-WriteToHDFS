/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package session7;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author majid
 */
//read from hdfs hadoop file system and save to it
public class spark{
    public static void main(String[] args) throws IOException {
        try {
            /*set master = "spark://master:9000/user/temp/4300-0.txt"*/
            SparkConf sparkConf = new SparkConf()
                    .setAppName("my App") // Set your application name
                    //.setMaster("spark://10.2.35.160:7077"); // Set the master URL (adjust the port if necessary)
                     .setMaster("local[*]") ;// Set the master URL (adjust the port if necessary)
                    //.set("spark.executor.memory", "512m") // Set executor memory
                    //.set("spark.driver.memory", "512m"); // Set driver memory

            //   SparkConf sparkConf = new SparkConf().setMaster("yarn").setAppName("My App");
            SparkContext sparkContext = new SparkContext(sparkConf);
            JavaRDD<String> lines = sparkContext.textFile(  "hdfs://10.2.35.160:8020/user/hadoop/sample.txt", 1).toJavaRDD();  /*10.2.35.160:9000(namenode ip port)*/
      /*      List<String> data = lines.collect();
            for (String line : data) {
                System.out.println(line);
            }
*/
            JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public Iterator<String> call(String t) throws Exception {
                    return Arrays.asList(t.split(" ")).iterator();
                }
            });

            System.out.println("*********************************part1*****************************");
            JavaPairRDD<String, Integer> pairwords = words.mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String t) throws Exception {
                    System.out.println("inside mapToPair : " + new Date().getTime());
                    return new Tuple2<>(t, 1);
                }int
            });
            System.out.println("*********************************Part2*****************************");
            JavaPairRDD<String, Integer> wordCount = pairwords.reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer t1, Integer t2) throws Exception {
                    System.out.println("inside reduceByKey : " + new Date().getTime());
                    return t1 + t2;
                }
            });
            //wordCount.repartition(1).saveAsTextFile("G:///BigData/BigDataWordCount-main/BigDataWordCount-main/output6");
            wordCount.repartition(1).saveAsTextFile("hdfs://10.2.35.160:8020/user/hadoop/output50" );/* args[0]*/  /*10.2.35.160:9000(namenode ip port)*/
            //wordCount.repartition(1).saveAsTextFile("hdfs://10.2.35.160:8020/user/hadoop/output3" );/* args[0]*/  /*10.2.35.160:9000(namenode ip port)*/
            System.out.println("*********************************Bye*****************************");

//        for (Tuple2<String,Integer> wordcount : wordCount.take(200)) {
//            System.out.println(wordcount);
//        }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }




/*
        Path p = new Path("hdfs://master:9000/user/temp/4300-0.txt");
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS", "hdfs://master:9000/root/temp/4300-0.txt");
        System.out.println(configuration.get("fs.defaultFS"));
        */
/*System.out.println(configuration.get("fs.default.name"));*//*


        FileSystem fs = FileSystem.get(configuration);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));

        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);

        }

//            BufferedReader reader = new BufferedReader(new FileReader("D:\\data\\data.csv"));
//            String line;
//            while ((line = reader.readLine()) != null) {
//                System.out.println(line);
//            }


            System.out.println("*********************************Hello*****************************");
        SparkConf conf = new SparkConf();
            conf.setAppName("wordCount");


        conf.setMaster("spark://master:9000/user/temp/4300-0.txt");
        */
/*https://itecno
te.com/tecnote/scala-error-initializing-sparkcontext-a-master-url-must-be-set-in-your-configuration/*//*

        JavaSparkContext context = new JavaSparkContext(conf);
        */
/*JavaRDD<String> lines = context.textFile("hdfs://newmaster:9000/user/root/wordCount");*//*

        JavaRDD<String> lines = context.textFile("hdfs://master:9000/user/temp/4300-0.txt");
//        JavaRDD<String> lines = context.mongo("hdfs://newmaster:9000/user/root/");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        });
*/

/*
        System.out.println("*********************************part1*****************************");
        JavaPairRDD<String, Integer> pairwords = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                System.out.println("inside mapToPair : " + new Date().getTime());
                return new Tuple2<>(t, 1);
            }
        });
        System.out.println("*********************************Part2*****************************");
        JavaPairRDD<String, Integer> wordCount = pairwords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer t1, Integer t2) throws Exception {
                System.out.println("inside reduceByKey : " + new Date().getTime());
                return t1 + t2;
            }
        });

//        for (Tuple2<String,Integer> wordcount : wordCount.take(200)) {
//            System.out.println(wordcount);
//        }
        wordCount.repartition(20).saveAsTextFile("hdfs://newmaster:9000/user/root/" + args[0]);
        System.out.println("*********************************Bye*****************************");
    }
*/

}
