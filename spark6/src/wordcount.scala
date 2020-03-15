import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object wordcount {
  
   def main(args:Array[String]){
    
    val config=new SparkConf().setAppName("wordcount prog").setMaster("local")
    val sc=new SparkContext(config)
    sc.setLogLevel("ERROR")
    
    val rdd=sc.textFile("F:\\data\\wordcount.txt")
    val rdd1=rdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((x,y)=>x+y)
    rdd1.foreach(println)
    
   }
  
}