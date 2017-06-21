import scala.util.matching.Regex
import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object GeneralReports {

    //def safe_date_from_str(input:String) : String = {
    //}
    
    //def safe_float(input:String) : Float ={
    //}

    // conf = SparkConf()
    // conf.setAppName("YOUR APP NAME HERE")
    // conf.setMaster("spark://compute-master:7077")
    // conf.set("spark.cores.max", "4")
    // conf.set("spark.shuffle.consolidateFiles", "true")
    // conf.set("spark.default.parallelism", "100")
    // conf.set("spark.executor.memory", "20g")
        
    // sc = SparkContext(conf=conf)
    def main(args: Array[String]) {
        val TOPIC = "missmalini"
        val TOPIC_NAME = "MissMalini"
        val REPORT_DIR = "/home/xiaoluguo/missmalini/"
        val DATA_ROOT = "hdfs://10.142.0.63:9000/data"
        val ARCHIVE_ROOT = "hdfs://10.142.0.63:9000/" + TOPIC
        val ENTITY_FILE = TOPIC + "_entities.txt"
        val SLICES = 2000

        var likeFacts = sc.textFile(DATA_ROOT+"/fact_like", SLICES).map(x => x.split("/t"))
            .filter(x => (x.length > 2))
            .map(x => (x(0),(x(1),x(2))))
            .distinct()

        val dimLikes = sc.textFile(DATA_ROOT+"/dim_like", SLICES).map(x => x.split("/t"))
            .filter(x => (x.length > 3))
            .setName("dimLikes")
            .cache()

        val iaFbMapB = sc.broadcast(dimLikes.map(x => (x(0), x(3))).distinct().collect().toMap)

        val likes = dimLikes.map(x => (x(0), (x(1), x(2)))).distinct()
        
        likes.saveAsTextFile("""/home/xiaoluguo/""")

        val safe_match: (String,String) => Boolean = (a:String,b:String) =>{
            if (a!=None){
                val sr ="miss".r.findFirstMatchIn(a.toLowerCase())
                val sr2="malini".r.findFirstMatchIn((a+b).toLowerCase())
                if (sr!=None && sr2!=None && sr.get.start< sr2.get.start){
                    true
                } else {
                    false
                }
            }else{
                false
            }

            // if (input._2._1!=None){
            //     val sr ="miss".r.findFirstMatchIn(input._2._1.toLowerCase())
            //     val sr2="malini".r.findFirstMatchIn((input._2._1+input._2._2).toLowerCase())
            //     if (sr!=None && sr2!=None && sr.get.start< sr2.get.start){
            //         true
            //     } else {
            //         false
            //     }
            // }else{
            //     false
            // }
        }

        val fbEntities = likes.filter(x=>true).collect()
        // fbEntities.saveAsTextFile("""/home/xiaoluguo/"""+ENTITY_FILE)
        val fp = new PrintWriter(new File(ENTITY_FILE))
        fp.write(fbEntities.mkString(""))
        fp.close()

        //val topicLikesB = sc.broadcast(topicLikes.map(x => x(0)).collect().toSet)

        // val manyTopicEntities = False

        // if (topicLikesB.value.size >= 1000){
        //     manyTopicEntities = True
        // }

        val locationFacts = sc.textFile(DATA_ROOT+ "/fact_location", SLICES).map(x => x.split("\t"))
            .filter(x => x.size > 2)
            .map(x => (x(2), x(1)))
            .distinct()
            .setName("location_facts")
            .cache()

        val indiaLocations = sc.textFile(DATA_ROOT+ "/dim_location", SLICES).map(x => x.split("\t"))
            .filter(x => x.size > 7)
            .filter(x => x(6) == "India")
            .filter(x => x(7) != "null")
            .map(x => (x(0),x(7)))
            .distinct()

        val anyIndiaLocations = sc.textFile(DATA_ROOT + "/dim_location", SLICES).map(x => x.split("\t"))
            .filter(x => x.size > 6)
            .filter(x => x(6) == "India")
            .map(x => (x(0),x(6)))
            .distinct()
            .setName("any_india_locations")
            .cache()
        
        val indiaPeople = locationFacts.join(anyIndiaLocations)
            .map(x=> (x._2._1, x._2._2))
            .setName("india_people")
            .distinct()
            .cache()
        
        val indiaPeopleStates = locationFacts.join(indiaLocations)
            .map(x => ((x._2)._1, (x._2)._2))
            .distinct()
            .cache()

        likeFacts = likeFacts.map(x => (x._2._1, (x._1, x._2._2)))
            .join(indiaPeople)
            .map(x => (x._2._1._1, (x._1, x._2._1._2)))
            .distinct()
            .coalesce(SLICES)
            .setName("like_facts")
            .cache()
        
        val personTotalLikeCounts = likeFacts.map(x => (x._2._1, 1))
            .reduceByKey((x,y) => (x+y))
            .distinct()
            .setName("personalTotalLikeCounts")
            .cache()

        personTotalLikeCounts.count()

    }
    // def safe_match(input:String){
    //     if (input(1)(0)!=None){
    //         sr ="miss".r.findFirstMatchIn(input(1)(0).toLowerCase())
    //         sr2="malini".r.findFirstMatchIn((input(1)(0)+input(1)(1)).toLowerCase())
    //         if (sr!=None && sr2!=None && sr.get.start< sr2. get.start) true else false
    //     }
    // }
}
