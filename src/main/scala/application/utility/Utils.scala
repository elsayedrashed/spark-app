package application.utility

object Utils {
  type ParamMap = Map[Symbol, String]

  def nextParam(map: ParamMap, list: List[String]): ParamMap = {
    def isSwitch(s: String) = s(0) == '-'

    list match {
      case Nil => map
      case "--spark-master" :: value :: tail =>
        nextParam(map ++ Map('sparkMaster -> value.toString), tail)
      case "--hive-dw-directory" :: value :: tail =>
        nextParam(map ++ Map('hiveDwDirectory -> value.toString), tail)
      case "--csv-delimiter" :: value :: tail =>
        nextParam(map ++ Map('csvDelimiter -> value.toString), tail)
      case "--top-n" :: value :: tail =>
        nextParam(map ++ Map('topN -> value.toString), tail)
      case "--anime-file" :: value :: tail =>
        nextParam(map ++ Map('animeFile -> value.toString), tail)
      case "--rating-file" :: value :: tail =>
        nextParam(map ++ Map('ratingFile -> value.toString), tail)
      case "--output-path" :: value :: tail =>
        nextParam(map ++ Map('outputPath -> value.toString), tail)
      case option :: tail => println("Unknown Parameter " + option)
        sys.exit(1)
    }
  }
}
