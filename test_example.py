from pyspark.sql import SparkSession
import uuid
import pytest
import tempfile
import random


@pytest.fixture(scope="module", autouse=True)
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


def test_save1(spark, tmp_path):
    df = spark.createDataFrame(
        [(i, uuid.uuid4().hex, random.random()) for i in range(500)],
        schema=["id", "value", "float"],
    )
    df.coalesce(1).write.format("parquet").mode("append").save(str(tmp_path))


def test_save2(spark, tmp_path):
    df = spark.createDataFrame(
        [(i, uuid.uuid4().hex, random.random()) for i in range(500)],
        schema=["id", "value", "float"],
    )
    df.coalesce(1).write.format("parquet").mode("append").save(str(tmp_path))
