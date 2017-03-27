// Spark implementation of job to count the number of times each
// unique IP address 4-tuple appears in an adudump file.
//

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.api.java.function.*;

import java.util.*;
import java.io.*;
//import java.net.*;
import java.text.*;

public final class InteractiveADU	 {//testing

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
    JavaPairRDD<String,long[]> maps = lines.flatMapToPair(
      new PairFlatMapFunction<String, String,long[]>() {
        @Override
        public Iterator<Tuple2<String,long[]>> call(String s) {
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
		   long bytes = Long.parseLong(tokens[5]);
              // eliminate the port part
              last_dot = IPaddr1.lastIndexOf('.');
              IPaddr1 = IPaddr1.substring(0, last_dot);
              last_dot = IPaddr2.lastIndexOf('.');
              IPaddr2 = IPaddr2.substring(0, last_dot);

              ArrayList<Tuple2<String,long[]>> Rec=new ArrayList<Tuple2<String,long[]>>();
	      if(direction.equals(">")){
				Rec.add(new Tuple2<String,long[]>(IPaddr1,new long[]{bytes,0}));//sent,rec
				Rec.add(new Tuple2<String,long[]>(IPaddr2,new long[]{0,bytes}));
			}
	      else if(direction.equals("<")){
				Rec.add(new Tuple2<String,long[]>(IPaddr2,new long[]{bytes,0}));
				Rec.add(new Tuple2<String,long[]>(IPaddr1,new long[]{0,bytes}));
		  }
              else{System.out.println("broken line");return Collections.emptyIterator();}

              return Rec.iterator();
        }
      }
    );


    //Create a PairRDD where each element has one key from a PairRDD and
    //a value which results from invoking the supplied function on all the
    //values that have that same key.  In this case, the value returned 
    //from the jth invocation is given as an input parameter to the j+1
    //invocation so a cumulative value is produced.
    JavaPairRDD<String, long[]> counts = maps.reduceByKey( //counts is unsorted all ADUs (String, long[])where long [] has two elems send,rec
      new Function2<long[], long[], long[]>() {//combine the long[]....
        public long[] call(long[] a1, long[] a2) {
          return new long[]{a1[0]+a2[0],a1[1]+a2[1]};
        }
      });
	  
	JavaPairRDD<Long,String> send_unsorted=counts.mapToPair(
		new PairFunction<Tuple2<String,long[]>,Long,String>(){
			public Tuple2<Long,String> call(Tuple2<String,long[]> foo)throws Exception{
				return new Tuple2<Long,String>(foo._2()[0],foo._1());
		 }
	 });
	 JavaPairRDD<Long,String> send_sorted=send_unsorted.sortByKey(false);//this is sent descended sorted (bytes,ip)
	 
	 JavaPairRDD<Long,String> rec_unsorted=counts.mapToPair(
		new PairFunction<Tuple2<String,long[]>,Long,String>(){
			public Tuple2<Long,String> call(Tuple2<String,long[]> foo)throws Exception{
				return new Tuple2<Long,String>(foo._2()[1],foo._1());
		 }
	 });
	 JavaPairRDD<Long,String> rec_sorted=rec_unsorted.sortByKey(false);//this is rec descended sorted (bytes,ip)
	 
	 
	
 Scanner in =new Scanner(System.in);
 	 counts.persist(StorageLevel.MEMORY_ONLY());//keep it stored...
	 send_sorted.persist(StorageLevel.MEMORY_ONLY());//keep it stored...
	 rec_sorted.persist(StorageLevel.MEMORY_ONLY());//keep it stored...
	 System.out.println("Collecting the results of the MapReduce...");


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
		int N,K;
		switch(decision){
			case 0: List<Tuple2<String,long[]>> output_0 = counts.collect();
					System.out.println("Writing it all to file..."+"This thing has "+output_0.size()+" entries so be patient :)");
					System.setOut(new PrintStream(new FileOutputStream("results"))); //print whole thing to file like in previous assignments
					for(Tuple2<String,long[]> tuple: output_0){
						System.out.println(tuple._1()+"\t"+tuple._2()[0]+"\t"+tuple._2()[1]);//toString representation should take care of this..
					}
					break;
			case 1: System.out.println("Please input your N <= 100");
					N = in.nextInt();
					if(N>100||N<1){System.out.println("Please enter a valid N...back to main menu.");break;}
					List<Tuple2<Long,String>> L_1=send_sorted.take(N);
					for (Tuple2<Long,String> tuple:L_1){
						System.out.println(tuple._2()+"\t"+tuple._1());
					}
					break;
			case 2: System.out.println("Please input your N <= 100");
					N = in.nextInt();
					if(N>100||N<1){System.out.println("Please enter a valid N...back to main menu.");break;}
					List<Tuple2<Long,String>> L_2=rec_sorted.take(N);
					for (Tuple2<Long,String> tuple:L_2){
						System.out.println(tuple._2()+"\t"+tuple._1());
					}
					break;
			case 3: System.out.println("Please input your K >= 1GB in units of MB");
					K = in.nextInt();
					final int num_3 = K;
					if(K<1000){System.out.println("Please enter a valid K...back to main menu.");break;}
					JavaPairRDD<Long,String> top_3=send_sorted.filter(
						new Function<Tuple2<Long,String>,Boolean>(){
							public Boolean call(Tuple2<Long,String> foo){
								if(foo._1()/1000>=num_3)return true;
								return false;
							}
					});
					List<Tuple2<Long,String>> L_3 = top_3.collect();
					for(Tuple2<Long,String>tuple:L_3){
						System.out.println(tuple._2()+"\t"+tuple._1());
					}
					break;
			case 4: System.out.println("Please input your K >= 100MB in units of MB");
					K = in.nextInt();
					final int num_4=K;
					if(K<100){System.out.println("Please enter a valid K...back to main menu.");break;}
					JavaPairRDD<Long,String> top_4=rec_sorted.filter(
						new Function<Tuple2<Long,String>,Boolean>(){
							public Boolean call(Tuple2<Long,String> foo){
								if(foo._1()/1000>=num_4)return true;
								return false;
							}
					});
					List<Tuple2<Long,String>> L_4 = top_4.collect();
					for(Tuple2<Long,String>tuple:L_4){
						System.out.println(tuple._2()+"\t"+tuple._1());
					}
					break;
			case 5: System.out.println("Please enter an IP addr...");
					String ip = in.next();
					if(ip.split("\\.").length!=4){System.out.println("malformed ip (omit the port)...back to main menu.");break;}
					List<long[]> vals_5=counts.lookup(ip);
					if(vals_5.size()==0)System.out.println("IP not found...back to main menu.");
					else if(vals_5.size()>1)System.out.println("WHAT?");
					else{
						long[] it=vals_5.get(0);
						System.out.println(ip+"\t"+it[0]+"\t"+it[1]);
					}
					break;
			case 6: sc.stop();System.out.println("Thanks for stopping by!"); System.exit(0);//test
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
			  Long.parseLong(tokens[5]);
		  }
		  catch(Exception e){
			  return false;
		  }
		  return true;
	  }
}
