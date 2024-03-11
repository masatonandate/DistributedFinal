import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

package FinalProject {

  import org.apache.spark.rdd.RDD

  object NewDriver {
    def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      // Setup Spark session
      val conf = new SparkConf().setAppName("NameOfApp")
        .setMaster("local[4]")
      val sc = new SparkContext(conf)

      val text = sc.textFile("adult.csv") // If running on server, put Hadoop input file path
      val header = text.first()
      // adjust the numeric data in RDD to be 4 categories representing percentiles
      val rows = text.filter(line => line != header)
        .map(line => line.split(","))
      // Array of (featureName, featureIdx, (25th, 50th, 75th) percentiles)
      val numericalFeatures = Array(
        ("fnlwgt", 3, find_percentiles(rows.map(r => r(3).toInt)))
      )
      val values = rows.map(r => {
        numericalFeatures.foreach(f => {
          r(f._2) = getKeyFromPercentile(r(f._2).toInt, f._3)
        })
        r
      })

      //Splitting Training and Testing (80/20)
      val trainTest = values.randomSplit(Array(0.8, 0.2), 1)
      val training = trainTest(0)
      val testing = trainTest(1)
      //val training = values.sample(withReplacement = false, fraction = 0.8)
      println("training length:" ,training.collect().length)
//      val testing = values.subtract(training)
      println("testing length:", testing.collect().length)
      println("Training Positive Length", training.filter(x => x(x.length - 1) == ">50K").collect().length)

      //getting the answers
      val testAnswers = testing.map(row => (row(0), row(row.length - 1)))
      val trainAnswers = training.map(row => (row(0), row(row.length - 1)))

      // rowNumber,age,workclass,fnlwgt,education,education-num,marital-status,occupation,relationship,race,sex,capital-gain,capital-loss,hours-per-week,native-country,income
      // (featureName, featureIdx)
      val categoricalFeatureNames = Array(("workclass", 2), ("education", 4), ("marital-status", 6), ("race", 9), ("sex", 10))

      //Build tree on train Data
      val decisionTree = NewDecisionTree(maxDepth = 5)
      val parentNode = decisionTree.create_tree(training, 0, categoricalFeatureNames, null)


//      decisionTree.recursive_print(parentNode)
      //Feed Test Data into Tree and get Results
      val testOutput = testing.map(x => (x(0), decisionTree.evaluate(x, parentNode)))
      val trainOutput = training.map(x => (x(0), decisionTree.evaluate(x, parentNode)))


      //computing F-Score for testAnswers
      val joinedAnsOut = testAnswers.join(testOutput)
//      joinedAnsOut.collect().foreach(println)
      val tp = joinedAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual == predicted && actual == ">50K" }).collect().length
      val fp = joinedAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual != predicted && predicted == ">50K" }).collect().length
      val fn = joinedAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual != predicted && predicted == "<=50K" }).collect().length
      val tn = joinedAnsOut
        .filter({case (rownum, (actual, predicted)) => actual == predicted && actual == "<=50K"}).collect().length

      val precision = tp.toDouble / (tp + fp)
      val recall = tp.toDouble / (tp + fn)

      val fscore = 2 * (precision * recall) / (precision + recall)
      println("Testing", fscore, precision, recall, tp, fp, fn, tn)

      //computing F-Score for train Answers
      val joinedTrainAnsOut = trainAnswers.join(trainOutput)
//      joinedTrainAnsOut.collect().foreach(println)
      val trtp = joinedTrainAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual == predicted && actual.equals(">50K") }).collect().length
      val trfp = joinedTrainAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual != predicted && predicted.equals(">50K") }).collect().length
      val trfn = joinedTrainAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual != predicted && predicted.equals("<=50K") }).collect().length
      val trtn = joinedTrainAnsOut
        .filter({case (rownum, (actual, predicted)) => actual == predicted && actual.equals("<=50K")}).collect().length

      val trprecision = trtp.toDouble / (trtp + trfp)
      val trrecall = trtp.toDouble / (trtp + trfn)

      val trfscore = 2 * (trprecision * trrecall) / (trprecision + trrecall)
      println("Training", trfscore, trprecision, trrecall, trtp, trfp, trfn, trtn)
    }

    def getKeyFromPercentile(value: Int, percentiles: (Int, Int, Int)): String = {
      if (value < percentiles._1) {
        "0"
      } else if (value < percentiles._2) {
        "1"
      } else if (value < percentiles._3) {
        "2"
      } else {
        "3"
      }
    }

    // Given feature of integers, finds 25th, 50th, and 75th percentiles
    def find_percentiles(data: RDD[Int]): (Int, Int, Int) = {
      val featureVals = data.collect().sorted(Ordering[Int])
      (featureVals((featureVals.length * 0.25).toInt), featureVals((featureVals.length * 0.5).toInt), featureVals((featureVals.length * 0.75).toInt))
    }
  }
}