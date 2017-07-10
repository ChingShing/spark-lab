import org.apache.spark.{SparkConf, SparkContext}

object TestScala {
  def select(): Unit = {
    val hdfsFilePath = "hdfs:///tbl/person.txt"
    val conf = new SparkConf()
    conf.setAppName("select")
    conf.setMaster("spark://172.17.0.2:8030")
    val sc = new SparkContext(conf)
    val textRdd = sc.textFile(hdfsFilePath, 3)
    val tblRdd = textRdd.map(line => line.split(","))
    val firstSelRdd = tblRdd.map(arrays => (arrays(1), arrays(2), arrays(3), arrays(4)))
    val calRdd = firstSelRdd.map(selData => {
      val nameValue = selData._1
      val genderValue = selData._2
      val heightValue = try {
        selData._3.toFloat
      }
      catch {
        case e: Throwable => 0f
      }
      val weightValue = try {
        selData._4.toFloat
      } catch {
        case e: Throwable => 0f
      }
      val tmpValue = if (weightValue != 0)
        weightValue
      else -1f;
      (tmpValue, (nameValue, genderValue))
    })
    val orderbyRdd = calRdd.sortByKey()
    val secondSelRdd = orderbyRdd.map(orderedData => (orderedData._2._1, orderedData._2._2))
    // fetch the result
    val result = secondSelRdd.collect()
    // print the result
    println("name\tgender")
    val iter = result.iterator
    while (iter.hasNext) {
      val resRow = iter.next()
      println(resRow._1 + "\t" + resRow._2)
    }
  }
}