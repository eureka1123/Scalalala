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
import org.apache.hadoop.fs.{FileSystem => FileSystemHadoop}
import org.apache.commons.lang3.text.WordUtils
import org.apache.spark.storage.StorageLevel._
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.StringBuilder
import org.apache.spark.rdd.RDD

object GeneralReports {

    def main(args: Array[String]) {
        // val conf = SparkConf()
        // conf.setAppName("YOUR APP NAME HERE")
        // conf.setMaster("spark://compute-master:7077")
        // conf.set("spark.cores.max", "52")
        // conf.set("spark.shuffle.consolidateFiles", "true")
        // conf.set("spark.default.parallelism", "100")
        // conf.set("spark.executor.memory", "20g")
            
        // val sc = SparkContext(conf=conf)

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
            .persist(MEMORY_ONLY_SER)

        // val iaFbMapB = sc.broadcast(dimLikes.map(x => (x(0), x(3))).distinct().collect().toMap)

        val likes = dimLikes.map(x => (x(0), (x(1), x(2)))).distinct()

        // THIS IS OLD, BUT CAN HELP FIND ENTITIES
        def evalEntity(fb_entities_file :Array[(String,(String, String))]): String = {

            val B = new StringBuilder

            for(entry <- fb_entities_file) {
                B ++= entry._1 + ","
                B ++= entry._2._1 + "," + entry._2._2 + "--"
            }

            B.deleteCharAt(B.length-1)
            val resultString = B.toString

            return resultString
        }

        def evalString(longString :String): ListBuffer[(String, (String, String))] = {

            val res : ListBuffer[(String, (String, String))] = ListBuffer()

            val splitString = longString.split("--")

            for (entry <- splitString){
                val internalSplit = entry.split(",")
                res += ((internalSplit(0),(internalSplit(1),internalSplit(2))))
            }

            return res
        }

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

        val fbEntities = likes.filter(x => safe_match(x._2._1,x._2._2)).collect()

        val fbEntitiesString = evalEntity(fbEntities)

        //human readable file
        val fp = new PrintWriter(new File(ENTITY_FILE))
        fp.write(fbEntitiesString)
        fp.close()

        val topicLikesString = Source.fromFile(ENTITY_FILE).mkString

        val topicLikes = sc.parallelize(evalString(topicLikesString).toList)

        val topicLikesB = sc.broadcast(topicLikes.map(x => x._1).collect().toSet)

        var manyTopicEntities = false

        if (topicLikesB.value.size >= 1000){
            manyTopicEntities = true
        }
        

        val conf = new Configuration()
        val hdfsCorePath = new Path("core-site.xml")
        val hdfsHDFSPath = new Path("hdfs-site.xml")
        conf.addResource(hdfsCorePath)
        conf.addResource(hdfsHDFSPath)
        val fileSystem = FileSystemHadoop.get(conf)
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
            .persist(MEMORY_ONLY_SER)

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
        
        val indiaPeople = locationFacts.join(anyIndiaLocations)
            .map(x=> (x._2._1, x._2._2))
            .setName("india_people")
            .distinct()
            .persist(MEMORY_ONLY_SER)
        
        val indiaPeopleStates = locationFacts.join(indiaLocations)
            .map(x => (x._2._1, x._2._2))
            .distinct()
            .persist(MEMORY_ONLY_SER)

        likeFacts = likeFacts.map(x => (x._2._1, (x._1, x._2._2)))
            .join(indiaPeople)
            .map(x => (x._2._1._1, (x._1, x._2._1._2)))
            .distinct()
            .coalesce(SLICES)
            .setName("like_facts")
            .persist(MEMORY_ONLY_SER)
            //.persist(MEMORY_ONLY_SER)
        
        val personTotalLikeCounts = likeFacts.map(x => (x._2._1, 1))
            .reduceByKey((x,y) => (x+y))
            .distinct()
            .setName("personalTotalLikeCounts")
            .persist(MEMORY_ONLY_SER)

        println(personTotalLikeCounts.count())

        var pathBigLikes = new Path("/" + TOPIC + "/big_likes")
        var fileExists = fileSystem.exists(pathBigLikes)

        var data = Array("1","2")
        var bigLikes = sc.parallelize(data)

        if (!fileExists) {
            bigLikes = likeFacts.map(x => (x._2._2, 1))
                .reduceByKey((x,y) => (x+y))
                .filter(x => x._2 > 999)
                .map(x => x._1)
                .coalesce(SLICES)
                .setName("bigLikes")
                .persist(MEMORY_ONLY_SER)
            bigLikes.saveAsTextFile("hdfs://10.142.0.63:9000/" + TOPIC + "/big_likes")
        } else {
            bigLikes = sc.textFile("hdfs://10.142.0.63:9000/" + TOPIC + "/big_likes")
                .setName("bigLikes")
                .persist(MEMORY_ONLY_SER)
        }

        // val topicLikesData = Array(("1",2),("3",4)) //placeholder
        // val topicLikes = sc.parallelize(topicLikesData)

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
            .persist(MEMORY_ONLY_SER)

        //(like_ID, user_ID)
        val targetedLikeFacts = likeFacts //#(fact_ID, (person_ID, like_ID))
            .map(x => (x._2._2, x._2._1))
            .setName("targetedLikeFacts")
            .persist(MEMORY_ONLY_SER)
        println(targetedLikeFacts.count())
        likeFacts.unpersist()

        //(person_ID, number_of_topic_and_big_likes) for people who like something big and/or topic
        val personRelevantLikeCounts = topicAndBigLikes
            .join(targetedLikeFacts) // (like_ID, ((name,type), user_ID))
            .map(x => (x._2._2, 1))
            .reduceByKey((x,y) => (x+y))
            .distinct()
            .coalesce(SLICES)
            .setName("personRelevantLikeCounts")
            .persist(MEMORY_ONLY_SER)

        //(like_ID, (1, user_ID)) for like_IDs in topic
        val topicLikeFacts = topicLikes
            .map(x => (x._1, 1))
            .join(targetedLikeFacts)
            .distinct()
            .setName("topicLikeFacts")
            .persist(MEMORY_ONLY_SER)

        //(person_ID, number_of_topic_likes) for people who like something in topic
        val personTopicLikeCounts = topicLikeFacts
            .map(x => (x._2._2, 1))
            .reduceByKey((x,y) => (x+y))
            .distinct()
            .setName("personTopicLikeCounts")
            .persist(MEMORY_ONLY_SER)

        //(person_ID, fraction of likes that are topic-related)
        val personTopicProportions = personRelevantLikeCounts
            .leftOuterJoin(personTopicLikeCounts)
            .map(x => (x._1, if (x._2._2.getClass.getSimpleName == None.getClass.getSimpleName) { 0.0 } else {x._2._2.get.toFloat/x._2._1}))
            .distinct()
            .setName("personTopicProportions")
            .persist(MEMORY_ONLY_SER)
    
        // This part is dealing with new data person_info
        //Lots of info, for people in India
        val people = sc.textFile(DATA_ROOT + "/person_info", SLICES).map(x => x.split("""\t"""))
            .filter(x => x.size > 8)
            .map(x => (x(0), x.drop(1)))
            .join(indiaPeople)
            .mapValues(x => x._1)
            .setName("people")
            .persist(MEMORY_ONLY_SER)

        val safe_date_from_str: ((String, Array[String])) => (String, Option[Calendar]) = (a:((String, Array[String]))) =>{
            val reg = """\d\d/\d\d/\d\d\d\d""".r
            var allMatch : Array[String] = reg.findAllIn(a._2(4)).toArray
            if (allMatch.length == 1) {
                var dateOB : Date = new Date(allMatch(0))

                var cal : Calendar = new GregorianCalendar()
                cal.setTime(dateOB)

                (a._1, Some(cal))

            } else {
                (a._1, None)
            }
        }
        val getAge: ((String, Option[Calendar])) => (String, Double) = (a:((String, Option[Calendar]))) => {
            var age: Double =0.0
            val milliInYear: Double =31557600000.0
            var str = a._1
            var date = a._2
            val today : Calendar = Calendar.getInstance()
            date match {
                case Some(date) => age= (today.getTime().getTime() - date.getTime().getTime())/milliInYear
                case None => val age= -1
            }

            (str,age)
        }

        //(person_ID, java calendar structure of birthday) - not actually age yet!
        var peopleBirthdays = people
            .filter(x => x._2(4) != "null")
            .map(safe_date_from_str)
            .filter(x => (x._2.getClass.getSimpleName != None.getClass.getSimpleName))
            .coalesce(SLICES)
            .setName("people_birthdays")
            .persist(MEMORY_ONLY_SER)
    
        //(person_ID, age in years)
        
        val peopleAges = peopleBirthdays
          .map(getAge)
          .coalesce(SLICES)
          .setName("people_ages")
          .persist(MEMORY_ONLY_SER)

        val get_ageband: ((String, Double)) => (String,String) = (a:((String, Double))) => {
            if (a._2 < 18) { (a._1, "Under 18") }
            if (a._2 < 25) { (a._1, "18-24") }
            if (a._2 < 35) { (a._1, "25-34") }
            if (a._2 < 45) { (a._1, "35-44") }
            if (a._2 < 55) { (a._1, "45-54") }
            (a._1, "55+")
        }

        val peopleAgebands = peopleAges
            .map(get_ageband)
            .persist(MEMORY_ONLY_SER)
        
        //(person ID, gender)
        val peopleGenders = people
            .filter(x => x._2(9) != "null")
            .map(x => (x._1, x._2(9)))
            .coalesce(SLICES)
            .distinct()
            .setName("people_genders")
            .persist(MEMORY_ONLY_SER)

        val clean_relationships: ((String, String)) => ((String, String)) = (rel:((String, String))) => {
            (rel._1, WordUtils.capitalize(rel._2).replaceAll("""\(Pending)\""","")) //does not include unicode, may need to be fixed
        }

        //(person ID, relationship status)
        //The isdigit filtering is because there are a bunch of relationship values that are dates.
        //Not sure why, or what it indicates, but for now I'm just ignoring them.
        val peopleRelationships = people
            .filter(x => x._2(7) != "null" && !(x._2(7)(0).isDigit))
            .map(x => (x._1, x._2(7)))
            .coalesce(SLICES)
            .map(clean_relationships)
            .distinct()
            .setName("people_relationships")
            .persist(MEMORY_ONLY_SER)
        //persist(MEMORY_ONLY_SER) only used here for debugging, shouldn't be needed?

        //(person ID, locale (i.e. language/country))
        val peopleLocales = people
            .filter(x => x._2(14) != "null")
            .map(x => (x._1, x._2(14)))
            .coalesce(SLICES)
            .distinct()
            .setName("people_locales")
            .persist(MEMORY_ONLY_SER)
        //persist(MEMORY_ONLY_SER) only used here for debugging, shouldn't be needed?
        
        //(person_ID, like_ID) for all big likes and all people who like them
        val bigLikeFacts = bigLikes.map(x => (x,1))
            .join(targetedLikeFacts)
            .map(x => (x._2._2, x._1))
            .persist(MEMORY_ONLY_SER)

        pathBigLikes = new Path("/" + TOPIC + "/like_topic_fractions")
        fileExists = fileSystem.exists(pathBigLikes)
        
        var data1 = Array(("1",2.0),("2",2.0))
        var likeTopicFractions = sc.parallelize(data1)

        val transform: (String) => (String, Double) = (str: String) =>{
            val index = str.indexOf("\'")
            val index2 = str.indexOf("\'", str.indexOf("\'") + 1)
            var a: String =  str.substring(index+1, index2)
            var b: Double = str.substring(str.indexOf(" "), str.length-1).toDouble
            (a,b) 
        }

        if (!fileExists) {
            //(like_ID, topic_like_proportion) for each person who likes like_ID (for big+topic likes)
            likeTopicFractions = bigLikeFacts.join(personTopicProportions)
                .map(x => (x._2._1, x._2._2))
                .persist(MEMORY_ONLY_SER)
            likeTopicFractions.saveAsTextFile("hdfs://10.142.0.63:9000/" + TOPIC + "/like_topic_fractions")
        } else{
            var pre = sc.textFile("hdfs://10.142.0.63:9000/" + TOPIC + "/like_topic_fractions").persist(MEMORY_ONLY_SER)//.map(eval).persist(MEMORY_ONLY_SER)) //fix eval
            likeTopicFractions = pre.map(transform)
        } 

        pathBigLikes = new Path("/" + TOPIC + "/like_topic_sum")
        fileExists = fileSystem.exists(pathBigLikes)

        var likeTopicSum = sc.parallelize(data1)

        if(!fileExists) {
            likeTopicSum = likeTopicFractions.reduceByKey((x,y) => (x+y)).persist(MEMORY_ONLY_SER) //This should give a measure of popularity of things, weighted by how big a topic fan each liker is. Normalizing it by count, as done below, gives a measure of strength of implication.
            likeTopicSum.saveAsTextFile("hdfs://10.142.0.63:9000/" + TOPIC + "/like_topic_sum")
        } else{
            var pre = sc.textFile("hdfs://10.142.0.63:9000/" + TOPIC + "/like_topic_sum")
            likeTopicSum = pre.map(transform)
        }

        //(like ID, number of likes for that ID) (for big+topic likes)
        val likeTopicCount = likeTopicFractions.map(x => (x._1, 1)).reduceByKey((x,y) => (x+y)).persist(MEMORY_ONLY_SER)

        //This is actually not just the strength normalized by count; it's also multiplied by an exponentiation (<1.0) of the count, so that more objectively popular things are weighted higher. Without this multiplication, we'd divide by x[1][1], by dividing by something less than that we're multiplying by the corresponding value. E.g., dividing by x[1][1]**0.7, we're weighting by popularity**0.3.
        //(like ID, topic-predictive power for that like ID)
        val likeTopicImplications = likeTopicSum.join(likeTopicCount).map(x => (x._1, x._2._1 / scala.math.pow(x._2._2, 0.7))).persist(MEMORY_ONLY_SER)

        //(like ID, (predictive power, (like name, like type))
        val sortedLikeTopicImplications = likeTopicImplications 
          .join(topicAndBigLikes) 
          .sortBy(x => x._2._1, false, SLICES) 
          .persist(MEMORY_ONLY_SER)

        val sortedOfftopicImplications = sortedLikeTopicImplications 
            .filter(x => !(topicLikesB.value.contains(x._1)))    
            .persist(MEMORY_ONLY_SER)

        //Make a file with the top 5000 putatively off-topic entities, for manual fixing
        // ot_file = codecs.open(TOPIC + "_top_offtopics.txt", "w", "utf-8")
        // ot_file.write(unicode(sorted_offtopic_implications.take(5000)))
        // ot_file.close()
    }
}

sortedLikeTopicImplications.saveAsTextFile("hdfs://compute-master:9000/" + TOPIC + "/like_topic_implications")

//just the like_IDs for the top 200 off-topic predictors
val topOffTopicsB = sc.broadcast((sortedOfftopicImplications.keys().take(200)).toSet)

val edgeLikeFacts = targetedLikeFacts.filter(x => topOffTopicsB.contains(x(0)))

val edgeLikers = edgeLikeFacts
    .values()
    .distinct()

val fanIDs = personTopicLikeCounts.keys()

//person_IDs who like top off-topic predictors but not any on-topic predictors
val edgeNonfans = personRelevantLikeCounts
    .keys()
    .substract(fanIDs)
    .intersection(edgeLikers)
    .persist(MEMORY_ONLY_SER)

//(person_ID, 1) for edge_nonfans
val edgeKv = edgeNonfans.map(x => (x,1)).persist(MEMORY_ONLY_SER)

//Takes an RDD of elements like (person_id, factor_value) and returns fandom sums for each factor value
def fandom_sums_by_factor(factorRdd: RDD[String]): RDD[String] = {
  dict(personTopicProportions 
    .join(factorRdd) 
    .map(x => (x._2._2, x._2._1)) 
    .reduceByKey((x,y)=>(x+y)) 
    .collect())
}

//for each factor, the number of topic fans
def fan_counts_by_factor(factorRdd: RDD[String]): RDD[String] = {
  dict(personTopicLikeCounts
    .join(factorRdd) 
    .map(x => (x._2._2, 1)) 
    .reduceByKey((x,y)=>(x+y)) 
    .collect())
}

//for each factor, the number of edge people
def edge_counts_by_factor(factorRdd: RDD[String]): RDD[String] = {
  dict(edgeKv
    .join(factorRdd) 
    .map(x => (x._2._2, 1)) 
    .reduceByKey((x,y)=>(x+y)) 
    .collect())
}

//for each factor, the number of total people
def person_counts_by_factor(factorRdd: RDD[String]): RDD[String] = {
  dict(personTotalLikeCounts
    .join(factorRdd) 
    .map(x => (x._2._2, 1)) 
    .reduceByKey((x,y)=>(x+y)) 
    .collect())
}

//for each factor, the total number of topic likes
def fan_like_counts_by_factor(factorRdd: RDD[String]): RDD[String] = {
  dict(personTopicLikeCounts
    .join(factorRdd)
    .map(x => (x._2._2, x._2._1)) 
    .reduceByKey((x,y)=>(x+y)) 
    .collect())
}

// #doesn't make sense - I must have thought this was looking at total like count or something
// def edge_like_counts_by_factor(factor_rdd):
//   return dict(person_topic_like_counts \
//     .join(edge_kv) \
//     .mapValues(lambda x: x[0]) \
//     .join(factor_rdd) \
//     .map(lambda x: (x[1][1], x[1][0])) \
//     .reduceByKey(add) \
//     .collect())

// //for each factor, the mean number of topic likes per person
// def mean_fan_like_counts_by_factor(factorRdd: RDD[String]): Array((Int,Float))
//   val flcbf = fan_like_counts_by_factor(factorRdd)
//   val fcbf = fan_counts_by_factor(factorRdd)
//   [(x, float(flcbf[x])/fcbf[x]) for x in fcbf if x in flcbf]

// // #doesn't make sense - I must have thought this was looking at total like count or something
// // def mean_edge_like_counts_by_factor(factor_rdd):
// //   elcbf = edge_like_counts_by_factor(factor_rdd)
// //   ecbf = edge_counts_by_factor(factor_rdd)
// //   return [(x, float(elcbf[x])/ecbf[x]) for x in ecbf if x in elcbf]

// //for each factor, the fraction of total population that is a fan
// def fan_concentration_by_factor(factor_rdd):
//   fcbf = fan_counts_by_factor(factor_rdd)
//   pcbf = person_counts_by_factor(factor_rdd)
//   return [(x, 100.0*fcbf[x]/pcbf[x]) for x in fcbf if x in pcbf]

// //for each factor, the fraction of total population that is an edge case
// def edge_concentration_by_factor(factor_rdd):
//   ecbf = edge_counts_by_factor(factor_rdd)
//   pcbf = person_counts_by_factor(factor_rdd)
//   return [(x, 100.0*ecbf[x]/pcbf[x]) for x in ecbf if x in pcbf]

// //for each factor, the mean fandom across fans at that factor level
// def fandom_by_factor(factor_rdd):
//   fsbf = fandom_sums_by_factor(factor_rdd)
//   fcbf = fan_counts_by_factor(factor_rdd)
//   return [(x, 100.0*fsbf[x]/fcbf[x]) for x in fsbf if x in fcbf]


val facebook_people = Array("Actor/director", "Artist", "Athlete", "Author", "Blogger", "Business person", "Chef", "Coach", "Dancer", "Designer", "Doctor", "Entertainer", "Entrepreneur", "Government official", "Journalist", "Lawyer", "Literary editor", "Monarch", "Musician/band", "News personality", "Personal blog", "Personal website", "Photographer", "Politician", "Producer", "Public figure", "Publisher", "Teacher", "Writer")
val facebook_people_b = sc.broadcast(facebook_people.toSet)


val facebook_music = Array("Album", "Arts/entertainment/nightlife", "Concert tour", "Concert venue", "Music", "Music award", "Music chart", "Music video", "Musical genre", "Musical instrument", "Musician/band", "Radio station", "Record label", "Song")
val facebook_music_b = sc.broadcast(facebook_music.toSet)


val facebook_new_media = Array("App", "App page", "Blogger", "Business/economy website", "Computers/internet website", "Education website", "Entertainment website", "Government website", "Health/wellness website", "Home/garden website", "Internet/software", "News/media website", "Personal blog", "Personal website", "Recreation/sports website", "Reference website", "Regional website", "Science website", "Society/culture website", "Teens/kids website", "Video game", "Website")
val facebook_new_media_b = sc.broadcast(facebook_new_media.toSet)


val facebook_old_media = Array("Article", "Author", "Book", "Book genre", "Book series", "Book store", "Entertainer", "Journalist", "Magazine", "Media/news/publishing", "Movie", "Movie character", "Movie general", "Movie genre", "Movie theater", "Museum/art gallery", "News personality", "Newspaper", "One-time tv program", "Performance art", "Photographer", "Publisher", "Radio station", "Record label", "Tv", "Tv channel", "Tv genre", "Tv network", "Tv season", "Tv show", "Tv/movie award")
val facebook_old_media_b = sc.broadcast(facebook_old_media.toSet)




/**
def sorted_category_subset_implications(category_subset_b):
  return sorted_like_topic_implications \
  .filter(lambda x: x[1][1][1] in category_subset_b.value)

bollywood_names = [u'Salman Khan', u'Amitabh Bachchan', u'Shah Rukh Khan', u'Mahendra Singh Dhoni', u'Akshay Kumar', u'Virat Kohli', u'Aamir Khan', u'Deepika Padukone', u'Hrithik Roshan', u'Sachin Tendulkar', u'Ranbir Kapoor', u'Priyanka Chopra', u'AR Rahman', u'Priety Zinta', u'Saif Ali Khan', u'Yo Yo Honey Singh', u'Sonakshi Sinha', u'Virender Sehwag', u'Shikhar Dhawan', u'Gautam Gambhir', u'Katrina Kaif', u'Kareena Kapoor Khan', u'Karan Johar', u'Madhuri Dixit', u'Ajay Devgn', u'Ravindra Jadeja', u'Sonu Nigam', u'Shreya Ghoshal', u'Suresh Raina', u'Mahesh Babu', u'Sonam Kapoor', u'Shahid Kapoor', u'Kapil Sharma', u'Arijit Singh', u'Yuvraj Singh', u'Farhan Akhtar', u'Ranveer Singh', u'Ajinkya Rahane', u'A. R. Murugadoss', u'Mika Singh', u'Vijay', u'Anushka Sharma', u'John Abraham', u'Sunny Leone', u'Rajinikanth', u'Bhuvneshwar Kumar', u'Harbhajan Singh', u'Ishant Sharma', u'Saina Nehwal', u'Rohit Sharma', u'Ajith Kumar', u'Abhishek Bachchan', u'Alia Bhatt', u'Parineeti Chopra', u'Sania Mirza', u'Sunidhi Chauhan', u'MC Mary Kom', u'Chetan Bhagat', u'Sanjay Leela Bhansali', u'Aishwarya Rai Bachchan', u'Vidya Balan', u'Jacqueline Fernandez', u'Arjun Kapoor', u'Kangana Ranaut', u'Varun Dhawan', u'Shraddha Kapoor', u'Bipasha Basu', u'Riteish Deshmukh', u'Anupam Kher', u'Rohit Shetty', u'Navjot Singh Sidhu', u'Nargis Fakhri', u'Anurag Kashyap', u'Pawan Kalyan', u'Shilpa Shetty', u'Imtiaz Ali', u'Anil Kapoor', u'Dhanush', u'Kirron Kher', u'Allu Arjun', u'Sukhwinder Singh', u'Shankar-Ehsaan-Loy', u'Hema Malini', u'Viswanathan Anand', u'Vishal-Shekhar', u'Shaan', u'Leander Paes', u'Prabhudheva', u'Rohan Bopanna', u'Sajid Nadiadwala', u'Anirban Lahiri', u'Mithun Chakraborty', u'Vir Das', u'Manish Paul', u'Malaika Arora Khan', u'Amish Tripathi', u'Ram Kapoor', u'Remo D’Souza', u"Remo D'Souza", u'Terence Lewis', u'Papa CJ']
bollywood_names_b = sc.broadcast(set([x.lower() for x in bollywood_names]))

def sorted_names_subset_implications(lowercase_name_subset_b):
  return sorted_like_topic_implications \
  .filter(lambda x: x[1][1][0].lower() in lowercase_name_subset_b.value)

**/

def sorted_category_subset_implications(category_subset_b: RDD[String]) = {
    sortedLikeTopicImplications.filter(x => category_subset_b.value.contains(x._2._2._2))
}


bollywood_names = Array("Salman Khan", "Amitabh Bachchan", "Shah Rukh Khan", "Mahendra Singh Dhoni", "Akshay Kumar", "Virat Kohli", "Aamir Khan", "Deepika Padukone", "Hrithik Roshan", "Sachin Tendulkar", "Ranbir Kapoor", "Priyanka Chopra", "AR Rahman", "Priety Zinta", "Saif Ali Khan", "Yo Yo Honey Singh", "Sonakshi Sinha", "Virender Sehwag", "Shikhar Dhawan", "Gautam Gambhir", "Katrina Kaif", "Kareena Kapoor Khan", "Karan Johar", "Madhuri Dixit", "Ajay Devgn", "Ravindra Jadeja", "Sonu Nigam", "Shreya Ghoshal", "Suresh Raina", "Mahesh Bab", "Sonam Kapoor", "Shahid Kapoor", "Kapil Sharma", "Arijit Singh", "Yuvraj Singh", "Farhan Akhtar", "Ranveer Singh", "Ajinkya Rahane", "A. R. Murugadoss", "Mika Singh", "Vijay", "Anushka Sharma", "John Abraham", "Sunny Leone", "Rajinikanth", "Bhuvneshwar Kumar", "Harbhajan Singh", "Ishant Sharma", "Saina Nehwal", "Rohit Sharma", "Ajith Kumar", "Abhishek Bachchan", "Alia Bhatt", "Parineeti Chopra", "Sania Mirza", "Sunidhi Chauhan", "MC Mary Kom", "Chetan Bhagat", "Sanjay Leela Bhansali", "Aishwarya Rai Bachchan", "Vidya Balan", "Jacqueline Fernandez", "Arjun Kapoor", "Kangana Ranaut", "Varun Dhawan", "Shraddha Kapoor", "Bipasha Bas", "Riteish Deshmukh", "Anupam Kher", "Rohit Shetty", "Navjot Singh Sidh", "Nargis Fakhri", "Anurag Kashyap", "Pawan Kalyan", "Shilpa Shetty", "Imtiaz Ali", "Anil Kapoor", "Dhanush", "Kirron Kher", "Allu Arjun", "Sukhwinder Singh", "Shankar-Ehsaan-Loy", "Hema Malini", "Viswanathan Anand", "Vishal-Shekhar", "Shaan", "Leander Paes", "Prabhudheva", "Rohan Bopanna", "Sajid Nadiadwala", "Anirban Lahiri", "Mithun Chakraborty", "Vir Das", "Manish Paul", "Malaika Arora Khan", "Amish Tripathi", "Ram Kapoor", "Remo D’Souza", "Remo D'Souza", "Terence Lewis", "Papa CJ")


var buffer = new ListBuffer[String]()

for (x <- bollywood_names){ 
    buffer += (x.toLowerCase())
}

val bollywood_names_b = sc.broadcast(buffer.toSet)

def sorted_names_subset_implications(lowercase_name_subset_b: RDD[String]) = {
    sorted_like_topic_implications.filter(x => lowercase_name_subset_b.value.contains(x._2._2._1.toLowerCase()))
}



