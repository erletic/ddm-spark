package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession, Encoder, Encoders}

object Sindy {
  // Define a kryo Encoder for (String, Set[String]) tuples
  implicit val tupleEncoder: Encoder[(String, Set[String])] = Encoders.tuple(Encoders.STRING, Encoders.kryo[Set[String]])

  // Read CSV data into a Spark Dataset
  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    // Convert each row into multiple (cellValue, Set(columnName)) tuples
    def extractDataFromRows(df: Dataset[Row]) = {
      val columns = df.columns
      df.flatMap(row => columns.indices.flatMap(i => Seq((row.get(i).toString, Set(columns(i))))))
    }

    // For each cellValue -> Set of columns, create (column, otherColumns) pairs
    def generateDependencyRows(set: (String, Set[String])) =
      set._2.map(identifier => (identifier, set._2 - identifier))

    // Main logic: read, transform, group, reduce, and collect results
    val finalINDs = inputs
      .map(input => readData(input, spark))
      .map(extractDataFromRows)
      .reduce(_ union _)
      .groupByKey(_._1) // group by cellValue
      .mapGroups((cellValue, tuples) => (cellValue, tuples.flatMap(_._2).toSet))
      .flatMap(generateDependencyRows)
      .groupByKey(_._1) // group by column
      .mapGroups { case (col, sets) => (col, sets.map(_._2).reduce(_ intersect _)) }
      .filter(_._2.nonEmpty)
      .collect()
      .map { case (col, deps) => (col, deps.toList.sorted) }
      .sortBy(_._1)

    // Print the discovered INDs
    finalINDs.foreach { case (column, dependencies) =>
      println(s"$column < ${dependencies.mkString(", ")}")
    }
  }
}
