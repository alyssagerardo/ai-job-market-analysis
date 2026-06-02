import org.apache.spark.{SparkConf, SparkContext}

object Main {

  case class Job(
    job_id: String,
    job_title: String,
    salary_usd: Double,
    salary_currency: String,
    experience_level: String,
    employment_type: String,
    company_location: String,
    company_size: String,
    employee_residence: String,
    remote_ratio: Double,
    required_skills: String,
    education_required: String,
    years_experience: Double,
    industry: String,
    posting_date: String,
    application_deadline: String,
    job_description_length: Double,
    benefits_score: Double,
    company_name: String
  )

  case class Job2(
    job: Job,
    features: Array[Double]
  )

  def parse_lines(line: String): Array[String] = {
    line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
      .map(_.trim.replaceAll("^\"|\"$", ""))
  }

  def normalize_features(data: Array[Job2]): Array[Job2] = {
    val num_features = data(0).features.length

    val min = Array.fill(num_features)(Double.MaxValue)
    val max = Array.fill(num_features)(Double.MinValue)

    for (point <- data) {
      for (i <- 0 until num_features) {
        if (point.features(i) < min(i)) {
          min(i) = point.features(i)
        }

        if (point.features(i) > max(i)) {
          max(i) = point.features(i)
        }
      }
    }

    data.map { point =>
      val normalized = point.features.indices.map { i =>
        if (max(i) == min(i)) {
          0.0
        } else {
          (point.features(i) - min(i)) / (max(i) - min(i))
        }
      }.toArray

      Job2(point.job, normalized)
    }
  }

  def distance(a: Array[Double], b: Array[Double]): Double = {
    math.sqrt(
      a.zip(b)
        .map { case (x, y) => math.pow(x - y, 2) }
        .sum
    )
  }

  def kmeans(data: org.apache.spark.rdd.RDD[Job2],
             k: Int,
             iterations: Int): org.apache.spark.rdd.RDD[(Job2, Int)] = {

    var centroids = data
      .takeSample(withReplacement = false, k, seed = 42)
      .map(_.features)

    for (iter <- 1 to iterations) {

      val assigned = data.map { point =>
        val cluster = centroids.indices.minBy(i =>
          distance(point.features, centroids(i))
        )

        (cluster, (point.features, 1))
      }

      val newCentroids = assigned
        .reduceByKey {
          case ((features1, count1), (features2, count2)) =>
            val summedFeatures = features1.zip(features2).map {
              case (x, y) => x + y
            }

            (summedFeatures, count1 + count2)
        }
        .mapValues {
          case (sumFeatures, count) =>
            sumFeatures.map(x => x / count)
        }
        .collect()
        .sortBy(_._1)

      centroids = newCentroids.map(_._2)

      println("Finished iteration " + iter)
    }

    data.map { point =>
      val cluster = centroids.indices.minBy(i =>
        distance(point.features, centroids(i))
      )

      (point, cluster)
    }
  }

  def to_job(cols: Array[String]): Job = {
    Job(
      cols(0),
      cols(1),
      cols(2).toInt,
      cols(3),
      cols(4),
      cols(5),
      cols(6),
      cols(7),
      cols(8),
      cols(9).toInt,
      cols(10),
      cols(11),
      cols(12).toInt,
      cols(13),
      cols(14),
      cols(15),
      cols(16).toInt,
      cols(17).toDouble,
      cols(18)
    )
  }

  def to_job2(job: Job): Job2 = {
    Job2(
      job,
      Array(
        job.salary_usd,
        job.remote_ratio,
        job.years_experience,
        job.benefits_score
      )
    )
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val file = sc.textFile("src/main/scala/ai_job_dataset.csv")
    val header = file.first()

    val rows = file
      .filter(line => line != header)
      .map(parse_lines)
      .filter(cols => cols.length >= 19)

    val jobs = rows.map(to_job)

    val jobs2 = jobs.map(to_job2)

    val normalized_array = normalize_features((jobs2.collect()))
    val normalized_job2 = sc.parallelize(normalized_array)

    val clustered = kmeans(normalized_job2, 4, 10)
    clustered.take(20).foreach {
      case (point, cluster) =>
        println(point.job.job_id + " -> cluster " + cluster)
    }
    sc.stop()
  }
}