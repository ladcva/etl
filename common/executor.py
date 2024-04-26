from abc import ABC, abstractmethod


class BaseExecutor(ABC):
    def __init__(self, spark, jdbc_url, user, password, driver):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.user = user
        self.password = password
        self.driver = driver

    @abstractmethod
    def read_dfs(self):
        raise NotImplementedError

    @staticmethod
    def write_df_to_parquet(df, target_path, partition_column):
        if df.count() > 0:
            df.write.partitionBy(partition_column).mode("overwrite").parquet(target_path)

    @abstractmethod
    def execute(self):
        raise NotImplementedError
    