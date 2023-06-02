from pyspark.sql import SparkSession
import uuid
import tempfile

with tempfile.TemporaryDirectory() as tempdir:
    with (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(tempdir))
        .getOrCreate()
    ) as sess:
        df = sess.createDataFrame(
            [(i, uuid.uuid4().hex) for i in range(500)],
            schema=["id", "value"],
        )
        df.coalesce(1).write.format("parquet").mode("append").save(tempdir)
