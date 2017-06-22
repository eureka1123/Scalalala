import scala.util.matching.Regex
import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Date
import java.util.Calendar
import java.util.GregorianCalendar
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

object GeneralReports {

    def main(args: Array[String]) {
        val TOPIC = "missmalini"
        val TOPIC_NAME = "MissMalini"
        val REPORT_DIR = "/home/xiaoluguo/missmalini/"
        val DATA_ROOT = "hdfs://10.142.0.63:9000/data"
        val ARCHIVE_ROOT = "hdfs://10.142.0.63:9000/" + TOPIC
        val ENTITY_FILE = TOPIC + "_entities.txt"
        val SLICES = 2000

        var likeFacts = sc.textFile(DATA_ROOT+"/fact_like", SLICES).map(x => x.split("""\t"""))
            .filter(x => (x.length > 2))
            .map(x => (x(0),(x(1),x(2))))
            .distinct()

        val dimLikes = sc.textFile(DATA_ROOT+"/dim_like", SLICES).map(x => x.split("""\t"""))
            .filter(x => (x.length > 3))
            .setName("dimLikes")
            .cache()

        val iaFbMapB = (dimLikes.map(x => (x(0), x(3))).distinct().collect().toMap)

        val likes = dimLikes.map(x => (x(0), (x(1), x(2)))).distinct()

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
        }

        val fbEntities = likes.filter(x=>safe_match(x._2._1,x._2._2)).collect()
        val fp = new PrintWriter(new File(ENTITY_FILE))
        fp.write(fbEntities.mkString(""))
        fp.close()

        //val topicLikesB = sc.broadcast(topicLikes.map(x => x(0)).collect().toSet)

        // val manyTopicEntities = False

        // if (topicLikesB.value.size >= 1000){
        //     manyTopicEntities = True
        // }

        private val conf = new Configuration()
        private val hdfsCorePath = new Path("core-site.xml")
        private val hdfsHDFSPath = new Path("hdfs-site.xml")
        conf.addResource(hdfsCorePath)
        conf.addResource(hdfsHDFSPath)
        private val fileSystem = FileSystem.get(conf)
        val path = new Path("/"+TOPIC)
        fileSystem.mkdirs(path) 


        val dir: File = new File(REPORT_DIR);
        val dir2: File = new File(REPORT_DIR+"/predictors");
        dir.mkdir();
        dir2.mkdir();


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

        var data = Array("1","2")
        var bigLikes = sc.parallelize(data)

        if (!fileExists) {
            bigLikes = likeFacts.map(x => (x._2._2, 1))
                .reduceByKey((x,y) => (x+y))
                .filter(x => x._2 > 999)
                .map(x => x._1)
                .coalesce(SLICES)
                .setName("bigLikes")
                .cache()

            bigLikes.saveAsTextFile("hdfs://10.142.0.63:9000/" + TOPIC + "/big_likes")
        } else {
            bigLikes = sc.textFile("hdfs://10.142.0.63:9000/" + TOPIC + "/big_likes")
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

        //(person_ID, number_of_topic_and_big_likes) for people who like something big and/or topic
        val personRelevantLikeCounts = topicAndBigLikes
            .join(targetedLikeFacts) // (like_ID, ((name,type), user_ID))
            .map(x => (x._2._2, 1))
            .reduceByKey((x,y) => (x+y))
            .distinct()
            .coalesce(SLICES)
            .setName("personRelevantLikeCounts")
            .cache()

        //(like_ID, (1, user_ID)) for like_IDs in topic
        val topicLikeFacts = topicLikes
            .map(x => (x._1, 1))
            .join(targetedLikeFacts)
            .distinct()
            .setName("topicLikeFacts")
            .cache()

        //(person_ID, number_of_topic_likes) for people who like something in topic
        val personTopicLikeCounts = topicLikeFacts
            .map(x => (x._2._2, 1))
            .reduceByKey((x,y) => (x+y))
            .distinct()
            .setName("personTopicLikeCounts")
            .cache()

        //(person_ID, fraction of likes that are topic-related)
        val personTopicProportions = personRelevantLikeCounts
            .leftOuterJoin(personTopicLikeCounts)
            .map(x => (x._1, if (x._2._2.getClass.getSimpleName == None.getClass.getSimpleName) { 0.0 } else {x._2._2.get.toFloat/x._2._1}))
            .distinct()
            .setName("personTopicProportions")
            .cache()
    
        // This part is dealing with new data person_info
        //Lots of info, for people in India
        val people = sc.textFile(DATA_ROOT + "/person_info", SLICES).map(x => x.split("""\t"""))
            .filter(x => x.size > 8)
            .map(x => (x(0), x.drop(1)))
            .join(indiaPeople)
            .mapValues(x => x._1)
            .setName("people")
            .cache()

        def safe_date_from_str(input : String) : Option[Calendar] = {
            val reg = """\d\d/\d\d/\d\d\d\d""".r
            var allMatch : Array[String] = reg.findAllIn(input).toArray
            if (allMatch.length == 1) {
                var dateOB : Date = new Date(allMatch(0))

                var cal : Calendar = new GregorianCalendar()
                cal.setTime(dateOB)

                return Some(cal)

            } else {
                return None
            }
        }

        def getAge( date : Option[Calendar]) : Double = {
            var age: Double =0.0
            val milliInYear: Double =31557600000.0
            
            val today : Calendar = Calendar.getInstance()
            date match {
                case Some(date) => age= (today.getTime().getTime() - date.getTime().getTime())/milliInYear
                case None => val age= -1
            }

            return age
        }

        //(person_ID, java calendar structure of birthday) - not actually age yet!
        val peopleBirthdays = people
            .filter(x => x._2(5) != "null")
            .map(x => (x._1, safe_date_from_str(x._2(5))))
            .filter(x => x._2.getClass.getSimpleName != None.getClass.getSimpleName)
            .coalesce(SLICES)
            .setName("people_birthdays")
            .cache()
    
        //(person_ID, age in years)
        
        val peopleAges = peopleBirthdays
          .map(x => (x._1, getAge(x._2)))
          .coalesce(SLICES)
          .setName("people_ages")
          .cache()

        def get_ageband(x: Double) : String = {
            if (x < 18) { return "Under 18" }
            if (x < 25) { return "18-24" }
            if (x < 35) { return "25-34" }
            if (x < 45) { return "35-44" }
            if (x < 55) { return "45-54" }
            return "55+"
        }

        val peopleAgebands = peopleAges
            .map(x => (x._1, get_ageband(x._2)))
            .cache()
        
        //(person ID, gender)
        val peopleGenders = people
            .filter(x => x._2(9) != "null")
            .map(x => (x._1, x._2(9)))
            .coalesce(SLICES)
            .distinct()
            .setName("people_genders")
            .cache()
    }
}