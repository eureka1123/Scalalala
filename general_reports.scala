import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object GeneralReports{
    val TOPIC = "missmalini"
    val TOPIC_NAME = "MissMalini"
    val REPORT_DIR = "/home/xiaoluguo/missmalini/"
    val DATA_ROOT = "hdfs://10.142.0.63:9000/data"
    val ARCHIVE_ROOT = "hdfs://10.142.0.63:9000/" + TOPIC
    val ENTITY_FILE = TOPIC + "_entities2.txt"
    val SLICES = 2000

    //def safe_date_from_str(input:String) : String = {
    //}
    
    //def safe_float(input:String) : Float ={
    //}

    def main(args: Array[String]) {

        var likeFacts = sc.textFile(DATA_ROOT+"/fact_like", SLICES).map(x => x.split("""\t"""))
            .filter(x => (x.length > 2))
            .map(x => (x(0),(x(1),x(2))))
            .distinct()

        val dimLikes = sc.textFile(DATA_ROOT+"/dim_like", SLICES).map(x => x.split("""\t"""))
            .filter(x => (x.length > 3))
            .setName("dimLikes")
            .cache()

        val iaFbMapB = sc.broadcast(dimLikes.map(x => (x(0), x(3))).distinct().collect().toMap)

        val likes = dimLikes.map(x => (x(0), (x(1), x(2)))).distinct()
    
        // ENTITY_FILE STUFF

        //val topicLikesB = sc.broadcast(topicLikes.map(x => x(0)).collect().toSet)

        // val manyTopicEntities = False

        // if (topicLikesB.value.size >= 1000){
        //     manyTopicEntities = True
        // }

        val locationFacts = sc.textFile(DATA_ROOT+ "/fact_location", SLICES).map(x => x.split("""\t"""))
            .filter(x => x.size > 2)
            .map(x => (x(2), x(1)))
            .distinct()
            .setName("location_facts")
            .cache()

        val indiaLocations = sc.textFile(DATA_ROOT+ "/dim_location", SLICES).map(x => x.split("""\t"""))
            .filter(x => x.size > 7)
            .filter(x => x(6) == "India")
            .filter(x => x(7) != "null")
            .map(x => (x(0),x(7)))
            .distinct()

        val anyIndiaLocations = sc.textFile(DATA_ROOT + "/dim_location", SLICES).map(x => x.split("""\t"""))
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

        val fileExists = true //need to write definition

        if (!fileExists) {
            val bigLikes = likeFacts.map(x => (x._2._2, 1))
                .reduceByKey((x,y) => (x+y))
                .filter(x => x._2 > 999)
                .map(x => x._1)
                .coalesce(SLICES)
                .setName("bigLikes")
                .cache()

            bigLikes.saveAsTextFile("hdfs://hadoopmaster:9000/' + TOPIC + '/big_likes")
        } else {
            val bigLikes = sc.textFile("hdfs://hadoopmaster:9000/' + TOPIC + '/big_likes")
                .setName("bigLikes")
                .cache()
        }

        val topicLikesData = Array(("1",2),("3",4)) //placeholder
        val topicLikes = sc.parallelize(topicLikesData)

        //(like_ID, (name, type))
        val topicAndBigLikes = topicLikes
            .map(x => x._1)
            .union(bigLikes)
            .map(x => (x, 1))
            .join(likes)
            .mapValues(x => x._2)
            .distinct()
            .coalesce(SLICES)
            .setName("topic_and_big_likes")
            .cache()

        //(like_ID, user_ID)
        val targetedLikeFacts = likeFacts //#(fact_ID, (person_ID, like_ID))
            .map(x => (x._2._2, x._2._1))
            .setName("targetedLikeFacts")
            .cache()
        targetedLikeFacts.count()
        likeFacts.unpersist()
    }
}