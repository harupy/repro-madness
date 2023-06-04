from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
import uuid
import pytest
import tempfile
import random
import pandas as pd

from typing import Iterator, Tuple
from pyspark.sql.functions import struct, col


@pytest.fixture(scope="class", autouse=True)
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


def test_save1(spark):
    @pandas_udf("double")
    def multiply(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.Series]:
        import datasets

        for df in iterator:
            yield df.sum(axis=1)

    num_columns = 6
    df = spark.createDataFrame(
        [tuple(random.random() for _ in range(num_columns)) for i in range(500)],
        schema=[str(i) for i in range(num_columns)],
    )
    df = df.withColumn("sum", multiply(struct([col(c) for c in df.columns])))
    df.show()


def test_save2(spark):
    @pandas_udf("double")
    def multiply(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.Series]:
        import datasets

        for df in iterator:
            yield df.sum(axis=1)

    num_columns = 6
    df = spark.createDataFrame(
        [tuple(random.random() for _ in range(num_columns)) for i in range(500)],
        schema=[str(i) for i in range(num_columns)],
    )
    df = df.withColumn("sum", multiply(struct([col(c) for c in df.columns])))
    df.show()
