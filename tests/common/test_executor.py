import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from abc import ABC, abstractmethod
from common.executor import BaseExecutor


class MockExecutor(BaseExecutor):
    def __init__(self, spark, jdbc_url, user, password, driver):
        super().__init__(spark, jdbc_url, user, password, driver)

    def read_dfs(self):
        return []

    def execute(self):
        pass


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("pytest-pyspark-example") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_base_executor_read_dfs(spark_session):
    mock_executor = MockExecutor(spark_session, "jdbc_url", "user", "password", "driver")
    result = mock_executor.read_dfs()
    assert result == []


def test_base_executor_write_df_to_parquet(spark_session):
    df = spark_session.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    target_path = "/tmp/test_parquet"
    partition_column = "id"
    BaseExecutor.write_df_to_parquet(df, target_path, partition_column)
    assert spark_session.read.parquet(target_path).count() == 2


def test_base_executor_execute(spark_session):
    mock_executor = MockExecutor(spark_session, "jdbc_url", "user", "password", "driver")
    mock_executor.execute()


def test_base_executor_write_df_to_parquet_with_partitioning(spark_session):
    data = [(1, "a", "2022-01-01"), (2, "b", "2022-01-01"), (3, "c", "2022-01-02")]
    df = spark_session.createDataFrame(data, ["id", "value", "date"])
    target_path = "/tmp/test_parquet_partitioned"
    partition_column = "date"
    BaseExecutor.write_df_to_parquet(df, target_path, partition_column)
    assert spark_session.read.parquet(target_path).count() == 3
    assert len(spark_session.read.parquet(target_path).drop_duplicates(["date"]).collect()) == 2


def test_base_executor_write_df_to_parquet_with_overwrite(spark_session):
    data = [(1, "a", "2022-01-01"), (2, "b", "2022-01-01"), (3, "c", "2022-01-02")]
    df = spark_session.createDataFrame(data, ["id", "value", "date"])
    target_path = "/tmp/test_parquet_overwrite"
    partition_column = "date"
    df.withColumn(partition_column, lit("2022-01-01")).write.mode("overwrite").partitionBy(partition_column).parquet(target_path)
    BaseExecutor.write_df_to_parquet(df, target_path, partition_column)
    assert spark_session.read.parquet(target_path).count() == 3
    assert len(spark_session.read.parquet(target_path).drop_duplicates(["date"]).collect()) == 2


def test_base_executor_execute_with_exception(spark_session):
    class FaultyExecutor(BaseExecutor):
        def read_dfs(self):
            raise Exception("Something went wrong")

        def execute(self):
            raise Exception("Something went wrong")

    faulty_executor = FaultyExecutor(spark_session, "jdbc_url", "user", "password", "driver")
    with pytest.raises(Exception):
        faulty_executor.read_dfs()
    with pytest.raises(Exception):
        faulty_executor.execute()
