from pyspark.sql import SparkSession


with (
    SparkSession.builder.master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
) as sess:
    df = sess.createDataFrame(
        [(1, "foo"), (2, "bar")],
        schema=["id", "value"],
    )
    df.write.format("parquet").mode("append").saveAsTable("delta")
