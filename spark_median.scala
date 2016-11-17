  case class Car(number: String, year: Int) {
    val region = number.drop(6)
  }

  def median(cars: RDD[Car]) = {
    cars
      .map(car => ((car.region, car.year), 1L))
      .reduceByKey(_ + _)
      .map { case ((region, year), count) =>
        (region, (year, count))
      }
      .groupByKey
      .map { case (region, countPerYear) =>
        (region, medianPerRegion(countPerYear))
      }.collectAsMap()
  }

  def medianPerRegion(countPerYear: Iterable[(Int, Long)]) = {
    recursiveMedian(countPerYear.toSeq.sortBy(_._1), countPerYear.map(_._2).sum)
  }

  def recursiveMedian(countPerYear: Iterable[(Int, Long)], total: Long, handled: Long = 0, prev: Int = 0): Int = {
    val middle = (total / 2.0).round
    if (handled >= middle) {
      if (total % 2 == 0) {
        val current = if (countPerYear.isEmpty || handled > middle) prev else countPerYear.head._1
        ((prev + current) / 2.0).round.toInt
      } else {
        prev
      }
    } else {
      val current = countPerYear.head
      recursiveMedian(countPerYear.tail, total, handled + current._2, current._1)
    }
  }

  "medianPerRegion for one year with 1 car" should "return just this year" in {
    assert(medianPerRegion(Seq((2014, 1L))) === 2014)
  }

  "medianPerRegion for one year with 3 cars" should "return just this year" in {
    assert(medianPerRegion(Seq((2014, 3L))) === 2014)
  }

  "medianPerRegion for one year with 4 cars" should "return just this year" in {
    assert(medianPerRegion(Seq((2014, 4L))) === 2014)
  }

  "medianPerRegion for three years with 1 car per year" should "return 2015 year" in {
    assert(medianPerRegion(Seq((2014, 1L), (2015, 1L), (2016, 1L))) === 2015)
  }

  "medianPerRegion for two years with 2 cars per first year and one car for last one" should "return 2014 year" in {
    assert(medianPerRegion(Seq((2014, 2L), (2015, 1L))) === 2014)
  }

  "medianPerRegion for two years with 1 car per first year and two cars for last one" should "return 2015 year" in {
    assert(medianPerRegion(Seq((2014, 1L), (2015, 2L))) === 2015)
  }

  "medianPerRegion for two years with 3 cars per first year and two cars for last one" should "return 2014 year" in {
    assert(medianPerRegion(Seq((2014, 3L), (2015, 2L))) === 2014)
  }

  "medianPerRegion for two years with 3 cars per first year and 1 car for last one" should "return 2014 year" in {
    assert(medianPerRegion(Seq((2014, 3L), (2015, 1L))) === 2014)
  }

  "medianPerRegion for two years with 1 car per first year and 3 cars for last one" should "return 2015 year" in {
    assert(medianPerRegion(Seq((2014, 1L), (2015, 3L))) === 2015)
  }

  "medianPerRegion for two years with 1 car per first year and 1 cars for last one" should "return 2015 year" in {
    assert(medianPerRegion(Seq((2014, 1L), (2016, 1L))) === 2015)
  }

  "Moscow has three items and NN just two" should " work" in {
    val cars = List(Car("a000aa77", 2014), Car("a001aa77", 2014), Car("a002aa77", 2015), Car("a000aa52", 2013), Car("a001aa52", 2015))
    assert(median(sc.parallelize(cars)) === Map("77" -> 2014, "52" -> 2014))
  }
