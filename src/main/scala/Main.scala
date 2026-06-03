import org.apache.spark.{SparkConf, SparkContext}

object Main {
  case class Job(
    jobID: String,
    jobTitle: String,
    salaryUSD: Double,
    salaryCurrency: String,
    experienceLevel: String,
    employmentType: String,
    companyLocation: String,
    companySize: String,
    employeeResidence: String,
    remoteRatio: Double,
    requiredSkills: String,
    educationRequired: String,
    yearsExperience: Double,
    industry: String,
    postingDate: String,
    applicationDeadline: String,
    jobDescriptionLength: Double,
    benefitsScore: Double,
    companyName: String
  )

  case class JobPoint(
    job: Job,
    features: Array[Double]
  )

  case class PCAResult(
    job: Job,
    cluster: Int,
    x: Double,
    y: Double
  )

  def parseLines(line: String): Array[String] = {
    line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
      .map(_.trim.replaceAll("^\"|\"$", ""))
  }

  def normalizeFeatures(data: Array[JobPoint]): Array[JobPoint] = {
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

      JobPoint(point.job, normalized)
    }
  }

  def distance(a: Array[Double], b: Array[Double]): Double = {
    math.sqrt(
      a.zip(b)
        .map { case (x, y) => math.pow(x - y, 2) }
        .sum
    )
  }

  def kmeans(data: org.apache.spark.rdd.RDD[JobPoint],
             k: Int,
             iterations: Int): org.apache.spark.rdd.RDD[(JobPoint, Int)] = {

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

  def toJob(cols: Array[String]): Job = {
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

  def tojobPoint(job: Job): JobPoint = {
    JobPoint(
      job,
      Array(
        job.salaryUSD,
        job.remoteRatio,
        job.yearsExperience,
        job.benefitsScore
      )
    )
  }

  def pca(clustered: Array[(JobPoint, Int)]): Array[PCAResult] = {
    val num_features = clustered(0)._1.features.length
    val means = Array.fill(num_features)(0.0)

    for ((point, cluster) <- clustered) {
      for (i <- 0 until num_features) {
        means(i) += point.features(i)
      }
    }
    for (i <- 0 until num_features) {
      means(i) = means(i) / clustered.length
    }

    clustered.map {
      case (point, cluster) =>
        val centered = point.features.indices.map { i =>
          point.features(i) - means(i)
        }.toArray

        PCAResult(
          point.job,
          cluster,
          centered(0),
          centered(2)
        )
    }
  }

  def silhouetteScore(clustered: Array[(JobPoint, Int)]): Double = {
    /*
    Silhouette Score

    For each point:
    a = average distance to points in same cluster
    b = average distance to points in nearest different clusters
    s = (b - a) / max(a, b)
    Average all s values to get the final silhouette score.

    After implementing function, test and jot down results for multiple k-values:
    k = 2, k = 3, k = 4, k = 5, k = 6
    For each k, run k-means function, compute the silhouette score, and record results.
    Chose k with the highest average silhouette score.
     */
    0.0
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val file = sc.textFile("data/ai_job_dataset.csv")
    val header = file.first()

    val rows = file
      .filter(line => line != header)
      .map(parseLines)
      .filter(cols => cols.length >= 19)

    val jobs = rows.map(toJob)

    val jobs2 = jobs.map(tojobPoint)

    val normalized_array = normalizeFeatures((jobs2.collect()))
    val normalized_job2 = sc.parallelize(normalized_array)

    // This is where you change the k value.
    val clustered = kmeans(normalized_job2, 4, 10)
    val pca_points = pca(clustered.collect())

    pca_points.take(20).foreach { p =>
      println(
        p.job.jobID +
        " -> cluster " + p.cluster +
        ", x = " + p.x +
        ", y = " + p.y
      )
    }
  }
}