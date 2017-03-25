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

public final class InteractiveADU {//test

  // The argument to the main function is the input file name
  // (specified as a parameter to the spark-submit command)
  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: InteractiveADU <file>");
      System.exit(1);
    }
	System.out.println("Welcome to InteractiveADU Featuring Spark :D");
	System.out.println("Reading in your super large file...");
		// Create a new Spark Context
	SparkConf conf = new SparkConf().setAppName("InteractiveADU");
	JavaSparkContext sc = new JavaSparkContext(conf);

	// Create a JavaRDD of strings; each string is a line read from
	// a text file.  
	JavaRDD<String> lines = sc.textFile(args[0]);
	System.out.println("Done!");
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
			  if(!isClean(tokens)){
					  System.out.println("broken line");return Collections.emptyIterator();
				}
              String IPaddr1 = new String();
              String IPaddr2 = new String();
             
              int last_dot;

              // get the two IP address.port fields
              IPaddr1 = tokens[2];
              IPaddr2 = tokens[4];

	      String direction = tokens[3];
	      

              // eliminate the port part
              last_dot = IPaddr1.lastIndexOf('.');
              IPaddr1 = IPaddr1.substring(0, last_dot);
              last_dot = IPaddr2.lastIndexOf('.');
              IPaddr2 = IPaddr2.substring(0, last_dot);

              String[] Rec=new String[1];
	      if(direction.equals(">"))Rec[0]=IPaddr2;
	      else if(direction.equals("<"))Rec[0]=IPaddr1;
              else{System.out.println("broken line");return Collections.emptyIterator();}

              return Arrays.asList(Rec).iterator();
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

 Scanner in =new Scanner(System.in);
	 System.out.println("Collecting the results of the MapReduce...");
	 List<Tuple2<String,Integer>> output = counts.collect();//if I put ADUInfo as other thing...I can now get compiler to tell me where to go from here
	while(true){ //loop after each query is finished...prompt for next command
		System.out.println("=======MAIN MENU=======");
		System.out.println("Select Op (type in number)");
		System.out.println("#0: Print the result of the reduced RDD...");
		System.out.println("#1: Top N IP addresses sending the largest number of bytes along w that byte count. N <= 100");
		System.out.println("#2: Top N IP addresses receiving the largest number of bytes along w that byte count. N <= 100");
		System.out.println("#3: The IP addresses sending more than K bytes along w that byte count. K >= 1GB");
		System.out.println("#4: The IP addresses receiving more than K bytes along w that byte count. K >= 100MB");
		System.out.println("#5: The number of bytes sent and received by a given IP address.");
		System.out.println("#6: exit.");
		int decision = in.nextInt();
		switch(decision){
			case 0: System.setOut(new PrintStream(new FileOutputStream("results"))); //print whole thing to file like in previous assignments
					for(Tuple2<?,?> tuple: output){
						System.out.println(tuple._1()+"\t"+tuple._2());
					}
					break;
			case 1: break;
			case 2: break;
			case 3: break;
			case 4: break;
			case 5: break;
			case 6: sc.stop();System.out.println("Thanks for stopping by!"); System.exit(0);
			default: System.out.println("Not a valid choice..."); break;
		}
		
    }
  }
  public static boolean isClean(String[] tokens){
		  if(tokens.length<6)return false;
		  if(!tokens[0].contains("ADU"))return false;
		  if((tokens[2].split("\\.")).length!=5)return false;
		  if(tokens[4].split("\\.").length!=5)return false;
		  if(!tokens[3].equals("<")&&!tokens[3].equals(">"))return false;
		  try{
			  Integer.parseInt(tokens[5]);
		  }
		  catch(Exception e){
			  return false;
		  }
		  return true;
	  }
}
