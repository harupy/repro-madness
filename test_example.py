from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
import pytest
import random
import pandas as pd

from typing import Iterator
from pyspark.sql.functions import struct, col


@pytest.fixture(scope="module")
def spark():
    with SparkSession.builder.master("local[*]").getOrCreate() as sess:
        yield sess


def test_pandas_udf(spark):
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
