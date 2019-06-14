package Spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import scala.Tuple2;

import static java.util.Locale.filter;

/**
 * Class App. Example on how to use Spark Map Transformation.
 *
 * @author denysche
 *
 */


public class App {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]")
                .set("spark.executor.memory","2g")
                .set("spark.driver.allowMultipleContexts", "true")
                .setAppName("SparkFileSumApp");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // read dataset into rdd
        JavaRDD<String> input = sc.textFile("/home/deny/Desktop/spark_dexploration/RDD.txt");

        List<Integer> collection = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //
        System.out.println(input.collect());

        //
        System.out.println(input.take(5));

        /**
         *  SparkMap
         */

        // flatMap()
        JavaRDD<String>  counts = input.flatMap(s -> Arrays.asList(s.split(",")).iterator());
        System.out.println(counts.collect());


        // map()
        JavaRDD<Integer> numb_int = counts.map(numberString -> Integer.valueOf(numberString));
        System.out.println(numb_int.collect());

        //
        JavaRDD<Integer> numb_add_1 = numb_int.map(y -> y+1);
        System.out.println(numb_add_1.collect());

        //
        JavaRDD<Integer> numb_sum = numb_int.map(y -> y);
        System.out.println(numb_add_1.collect());

        // parallelize()
        JavaRDD<Integer> rdd = sc.parallelize(collection, 2);
        System.out.println("Number of partitions : "+rdd.getNumPartitions());

        // sum elemnts
        JavaRDD<Integer> lineLengths = counts.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println(totalLength);

        // map tuple
        JavaPairRDD<String, Long> pairs =  counts.mapToPair(s -> new Tuple2<>(s, 1L));
        System.out.println(pairs.collect());

        // map lambda expresion
        JavaPairRDD<Integer, Long> rdd1 = sc.parallelize(Arrays.asList(1, 2, 1, 0)).zipWithIndex();
        rdd1.mapToPair(x -> { if (x._1 == 2) return new Tuple2<Integer, Long>(x._1*x._1, x._2); else return new Tuple2<Integer, Long>(x._1, x._2);
        }).foreach(x -> System.out.println(x));

        //Reduce Function for cumulative sum
        Function2<Integer, Integer, Integer> reduceSumFunc = (accum, n) -> (accum + n);
        JavaRDD<Integer> rddX = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);
        Integer cumsum = rddX.reduce(reduceSumFunc);
        System.out.println(cumsum);


        rddX.foreach(item -> { System.out.println("* "+item*1); });


        /**
         *  flatMap(func)
         */



        /**
         *  read hdfs data
         */


        JavaRDD<Integer> rdd_list = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);
        System.out.println(rdd_list.collect());


        hadoop hdfsData = new hadoop(sc);
        JavaRDD<String> carData = hdfsData.readHDFS("hdfs://localhost:5432/CarDetails/part-00000")
                .filter(x -> x.contains("audi"));
        hdfsData.saveToHDFS(carData, "hdfs://localhost:5432/CarDetails/Audi");

        sc.close();

    }
}