// Spark implementation of job to count the number of times each
// unique IP address 4-tuple appears in an adudump file.
//

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.io.*;
//import java.net.*;
import java.text.*;

public final class JavaHostCount {

  // The argument to the main function is the input file name
  // (specified as a parameter to the spark-submit command)
  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaHostCount <file>");
      System.exit(1);
    }

    // Create a new Spark Context
    SparkConf conf = new SparkConf().setAppName("HostCount");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Create a JavaRDD of strings; each string is a line read from
    // a text file.  
    JavaRDD<String> lines = sc.textFile(args[0]);


    // The flatMap operation applies the provided function to each
    // element in the input RDD and produces a new RDD consisting of
    // the strings returned from each invocation of the function.
    // The strings are returned to flatMap as an iterator over a
    // list of strings (string array to string list with iterator). 
    JavaRDD<String> words = lines.flatMap(
      new FlatMapFunction<String, String>() {
        @Override
        public Iterator<String> call(String s) {
              String[] tokens = s.split(" ");
              String IPaddr1 = new String();
              String IPaddr2 = new String();
             
              int last_dot;

              // get the two IP address.port fields
              IPaddr1 = tokens[2];
              IPaddr2 = tokens[4];

              // eliminate the port part
              last_dot = IPaddr1.lastIndexOf('.');
              IPaddr1 = IPaddr1.substring(0, last_dot);
              last_dot = IPaddr2.lastIndexOf('.');
              IPaddr2 = IPaddr2.substring(0, last_dot);

              String[] hosts = {IPaddr1, IPaddr2};

              return Arrays.asList(hosts).iterator();
        }
      }
    );


    //Create a PairRDD of <Key, Value> pairs from an RDD.  The input RDD
    //contains strings and the output pairs are <String, Integer>. 
    //The Tuple2 object is used to return the pair.  mapToPair applies
    //the provided function to each element in the input RDD. 
    JavaPairRDD<String, Integer> ones = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String s) {
	    return new Tuple2<>(s, 1); //key = IP, value = 1
        }
      });

    //Create a PairRDD where each element has one key from a PairRDD and
    //a value which results from invoking the supplied function on all the
    //values that have that same key.  In this case, the value returned 
    //from the jth invocation is given as an input parameter to the j+1
    //invocation so a cumulative value is produced.
    JavaPairRDD<String, Integer> counts = ones.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });

    //Java to redirect stdout to a named file
    System.setOut(new PrintStream(new FileOutputStream("hostcounts")));
    //collect all the reduced <K,V> pairs as a list of Tuple2 objects
    //and output each as a line in the file
    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      //the Tuple2 methods ._1() and ._2() get the pair 1st and 2nd values
      System.out.println(tuple._1() + ": " + tuple._2());
    }
    
    sc.stop();
  }
}
