rm *.class
javac -classpath .:/var/spark/jars/spark-core_2.11-2.0.2.jar:/var/spark/jars/spark-sql_2.11-2.0.2.jar:/var/spark/jars/scala-library-2.11.8.jar:/var/hadoop/share/hadoop/common/hadoop-common-2.7.3.jar:/var/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.3.jar $1.java
jar cf $1.jar *.class

