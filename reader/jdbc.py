from abc import ABC, abstractmethod

class JDBCReader(ABC):
    def __init__(self, spark, host, port, database, user, password, driver):
        self.spark = spark
        self.jdbc_url = self.construct_jdbc_url(host, port, database)
        self.user = user
        self.password = password
        self.driver = driver

    @abstractmethod
    def read_table(self, table_name):
        pass

    @staticmethod
    def construct_jdbc_url(host, port, database):
        return f"jdbc:mysql://{host}:{port}/{database}?permitMysqlScheme"

class MariaDbJDBCReader(JDBCReader):
    def __init__(self, spark, host, port, database, user, password, driver):
        super().__init__(spark, host, port, database, user, password, driver)

    def read_table(self, table_name):
        return self.spark.read.format("jdbc") \
            .options(url=self.jdbc_url, dbtable=table_name, user=self.user, password=self.password, driver=self.driver) \
            .load()
    