from pyspark.sql import SparkSession
import uuid
import pytest
import tempfile


@pytest.fixture(scope="module")
def spark():
    with tempfile.TemporaryDirectory() as tmpdir:
        with (
            SparkSession.builder.master("local[*]")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", str(tmpdir))
            .getOrCreate()
        ) as sess:
            yield sess


def test_save(spark, tmp_path):
    df = spark.createDataFrame(
        [(i, uuid.uuid4().hex) for i in range(500)],
        schema=["id", "value"],
    )
    df.coalesce(1).write.format("parquet").mode("append").save(str(tmp_path))
