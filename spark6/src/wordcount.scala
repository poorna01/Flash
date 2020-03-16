import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object wordcount {
  
   def main(args:Array[String]){
    
     println("--------")
    val config=new SparkConf().setAppName("wordcount prog").setMaster("local")
    val sc=new SparkContext(config)
    sc.setLogLevel("ERROR")
    
    val rdd=sc.textFile("F:\\data\\wordcount.txt")
    val rdd1=rdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((x,y)=>x+y)
    rdd1.foreach(println)
    
    println("--------")
    
     val rdd2=sc.textFile("F:\\data\\wordcount1.txt")
    val rdd3=rdd2.flatMap(x=>x.split(",")).map(x=>(x,1)).reduceByKey((x,y)=>x+y)
     rdd3.foreach(println)
     
     println("---------")
     val rdd4=sc.textFile("F:\\data\\wordcount1.txt")
     val rdd5=rdd4.map(x=>x.split(",")).map(x=>(x(0),x(1)))
     rdd5.foreach(println)
    
   }
  
}