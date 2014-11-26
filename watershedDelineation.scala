import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection._
import scala.collection.mutable.Seq
import scala.util.Random
import scala.math._


object watershedDelineation {
   def main(args : Array[String]) : Unit = {
    val sc = new SparkContext("local", "watershed")
    val temp_data = sc	.textFile("/home/kaushik/Desktop/lol2.csv")
    					.map(word => 	((word	.split(","))
    										.map(lit => lit.toDouble)
    										.toList
    									).toVector)
    					.map(word => word(0) -> (word(1) -> word(2)))
    					
    val x_len = temp_data.groupBy(f => f._1).count
    val y_len = temp_data.groupBy(f => f._2._1).count

    val data = temp_data.groupByKey.sortByKey(true)
    			.zipWithIndex
    			.map(word => word._1._2.map(w => w._1 -> (word._2 -> w._2)) ).flatMap(f => f)
    			.groupByKey.sortByKey(true)
    			.zipWithIndex
    			.map(word => word._1._2.map(w => (w._1 -> word._2) -> w._2)).flatMap(f => f)
    data.saveAsTextFile("/home/kaushik/Desktop/flow")
    
    val mod_data1 = data.map(word => ((word._1._1 + 1),word._1._2) -> word)
    val mod_data2 = data.map(word => ((word._1._1 - 1),word._1._2) -> word)
    val mod_data3 = data.map(word => (word._1._1,(word._1._2 - 1)) -> word)
    val mod_data4 = data.map(word => (word._1._1,(word._1._2 + 1)) -> word)
    val mod_data5 = data.map(word => ((word._1._1 + 1),(word._1._2 - 1)) -> word)
    val mod_data6 = data.map(word => ((word._1._1 + 1),(word._1._2 + 1)) -> word)
    val mod_data7 = data.map(word => ((word._1._1 - 1),(word._1._2 - 1)) -> word)
    val mod_data8 = data.map(word => ((word._1._1 - 1),(word._1._2 + 1)) -> word)
    
    
    val mod_data = mod_data1.union(mod_data2).union(mod_data3).union(mod_data4)
    						.union(mod_data5).union(mod_data6).union(mod_data7)
    						.union(mod_data8).filter(word => (word._1._1 >= 0) && (word._1._1 < x_len))
    						.filter(word => (word._1._2 >= 0) && (word._1._2 < y_len))
    						.groupByKey
    						.sortByKey(true)
    						.map(word => word._1 -> {
    						  
    						  /**
    						   * (OLD)
    						   * returns the minimum nearest list of points
    						   */
//    						  val x = word._2.minBy(f => f._2)
//    						  word._2.filter(w => w._2 == x._2)
    						  
    						  
    						  /**
    						   * (NEW)
    						   * let it return all the nearest set of points
    						   */
    						  word._2
    						  })
    
    val join = data.join(mod_data)
   // val flow_direction = join.map(word => (word._1._1 -> word._1._2) -> Direction(word._1._1, word._1._2, word._2._1, word._2._2._1._1, word._2._2._1._2, word._2._2._2))

    /**
     * (OLD)
     * gives directions to all the "min. nearest list of points"
     */
//         val flow_direction = join.map(word => (word._1._1 -> word._1._2) -> word._2._2.map(w => Direction(word._1._1, word._1._2, word._2._1, w._1._1, w._1._2, w._2)))
//     							.coalesce(1)
    
    /**
     * (NEW)
     * will filter out all points less than or equal to the points value and then apply map
     */
    val flow_direction = join.map(word => (word._1._1 -> word._1._2) -> 
    										word._2._2.map(w => Direction(word._1._1, word._1._2, word._2._1, w._1._1, w._1._2, w._2)->0)
    										.groupBy(f => f._1).keys
    							)
     							.coalesce(1)
    
    flow_direction.saveAsTextFile("/home/kaushik/Desktop/flowDirection")
    
    
    
    val temp = flow_direction.filter(word => word._2.toSeq.contains('O')).first._1
    val x = temp._1
    val y =  temp._2
    
    
    
    var alterData = flow_direction.map(word => if((word._1._1 == x) && (word._1._2 == y)) {word -> 1} else {word -> 0} )
    var count = alterData.filter(word => word._2 == 1).count
//    for(i <- 0 to 0) {
    while(count > 0) {
      val data_occurence1 = alterData.filter(word => word._2 == 1)
      val data_occurence0 = alterData.filter(word => word._2 == 0)
      val data_occurence2 = alterData.filter(word => word._2 == 2)
      
      val data1 = data_occurence1.map(word => ((word._1._1._1 + 1) -> word._1._1._2) -> 'N')
      val data2 = data_occurence1.map(word => ((word._1._1._1 - 1) -> word._1._1._2) -> 'S')
      val data3 = data_occurence1.map(word => (word._1._1._1 -> (word._1._1._2 - 1)) -> 'E')
      val data4 = data_occurence1.map(word => (word._1._1._1 -> (word._1._1._2 + 1)) -> 'W')
      val data5 = data_occurence1.map(word => ((word._1._1._1 - 1) -> (word._1._1._2 - 1)) -> 'B')
      val data6 = data_occurence1.map(word => ((word._1._1._1 - 1) -> (word._1._1._2 + 1)) -> 'C')
      val data7 = data_occurence1.map(word => ((word._1._1._1 + 1) -> (word._1._1._2 - 1)) -> 'A')
      val data8 = data_occurence1.map(word => ((word._1._1._1 + 1) -> (word._1._1._2 + 1)) -> 'D')
      
      val data_union = data1.union(data2).union(data3)
      						.union(data4).union(data5)
      						.union(data6).union(data7).union(data8)
      						.filter(word => (word._1._1 >= 0) && (word._1._1 < x_len))
    						.filter(word => (word._1._2 >= 0) && (word._1._2 < y_len))
    						.groupByKey
//    						.map(word => word._1 -> word._2.toSeq.contains(elem))
    //data_union.saveAsTextFile("/home/kaushik/Desktop/join")
     val temp_data0 = data_occurence0.map(word => (word._1._1) -> (word._1._2 ))
     //cells with value '1'
     val join0 = temp_data0	.join(data_union)
     						.map(word => (word._1 -> word._2._1) -> {
     										if(word._2._1.toSeq	.map(f => if(word._2._2.toSeq.contains(f)) 1 else 0)
     															.filter(d => d==1).length > 0) 1 else 0
     															})
     						.filter(word => word._2 == 1)

     val tData_occurence0 = data_occurence0.subtractByKey(join0)
     
     
     //cells with value '1' converted to 2
     val join1 = data_occurence1.map(word => word._1 -> 2)
     
//     join1.saveAsTextFile("/home/kaushik/Desktop/2")
//     join0.saveAsTextFile("/home/kaushik/Desktop/1_0")
//     temp_data0.saveAsTextFile("/home/kaushik/Desktop/0")
     
     alterData = tData_occurence0.union(join0).union(join1).union(data_occurence2)
    //alterData = join0.union(join1).union(data_occurence2)
     alterData = alterData.coalesce(1)
     alterData.persist
     count = alterData.filter(word => word._2 == 1).count
     
    }
    
  
    alterData.map(word => word._1._1._1 + "," + word._1._1._2 + "," + word._2).coalesce(1).saveAsTextFile("/home/kaushik/Desktop/result2")//.foreach(word => println(word._1 + " : " + word._2))
//    alterData.map(word => word._2 -> (word._1._1._1 + "," + word._1._1._2) ).coalesce(10).sortByKey(false).coalesce(1).saveAsTextFile("/home/kaushik/Desktop/result2")
    //.sortBy(w => , ascending, numPartitions).coalesce(1).saveAsTextFile("/home/kaushik/Desktop/result2")//.foreach(word => println(word._1 + " : " + word._2))
    //.coalesce(1)
    
    println("x : "+x_len+" y : "+y_len)
    println("pos : " + x + "," + y)
   }
   
   def Direction(x:Long, y:Long, z:Double, a:Long, b:Long, c:Double): Char = {
     if(z < c) return 'O'
     else {
    	 if((x-a == 1)&&(y-b == 1)) return 'D'
    	 if((x-a == 1)&&(y-b == 0)) return 'N'
    	 if((x-a == 1)&&(y-b == -1)) return 'A'
    	 if((x-a == 0)&&(y-b == -1)) return 'E'
    	 if((x-a == -1)&&(y-b == -1)) return 'B'
    	 if((x-a == -1)&&(y-b == 0)) return 'S'
    	 if((x-a == -1)&&(y-b == 1)) return 'C'
    	 if((x-a == 0)&&(y-b == 1)) return 'W'
       
     }
     return 'O'
     
   }

}
