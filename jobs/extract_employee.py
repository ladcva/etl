import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

from common.executor import BaseExecutor
from reader.jdbc import MariaDbJDBCReader


DATE_FORMAT = "yyyy-MM-dd"


class JobExecutor(BaseExecutor):
    def __init__(self, spark, jdbc_reader, cols, input_tables, target):
        self.spark = spark
        self.jdbc_reader = jdbc_reader
        self.cols = cols
        self.input_tables = input_tables
        self.target = target
        self.partition_col = "partition_date"

    def read_dfs(self):
        return [self.jdbc_reader.read_table(table['table']) for table in self.input_tables]

    def execute(self):
        dfs = self.read_dfs()
        df_employee, df_rate = dfs[0], dfs[1]

        df_employee_filtered = self._transform_employee(df_employee)
        df_transformed = self._join_data(df_employee_filtered, df_rate)

        df_transformed.show()

        self.write_df_to_parquet(df_transformed, self.target, partition_column=self.partition_col)

    def _transform_employee(self, df_employee):
        return df_employee.select(*self.cols) \
            .filter(F.col("SalariedFlag") == True) \
            .filter(F.col("OrganizationLevel").isNotNull()) \
            .withColumn("MaritalStatus", F.when(F.col("MaritalStatus") == "M", "Married")
                                          .when(F.col("MaritalStatus") == "S", "Single")
                                          .otherwise("Other")) \
            .withColumn("Gender", F.when(F.col("Gender") == "M", "Male")
                                    .when(F.col("Gender") == "F", "Female")
                                    .otherwise("Other"))

    def _join_data(self, df_employee_filtered, df_rate):
        df_rate.printSchema()
        df_joined = df_employee_filtered.alias("r").join(
            df_rate.alias("l"),
            how="left",
            on=(F.col("r.BusinessEntityID") == F.col("l.BusinessEntityID"))
        ).withColumn("partition_date", F.to_date(F.col("r.ModifiedDate"), DATE_FORMAT)).drop("ModifiedDate")
        
        df_joined = df_joined.select(
            "r.*",
            "l.Rate",
            "l.PayFrequency",
            "partition_date"
        )

        return df_joined
        

if __name__ == "__main__":
    load_dotenv()

    # Get database connection details from environment variables
    configurations = {
        "appName": "PySpark Example - MariaDB Example",
        "master": "local",
        "database": os.getenv("DATABASE_NAME"),
        "host": os.getenv("DATABASE_HOST"),
        "port": int(os.getenv("DATABASE_PORT")),
        "user": os.getenv("DATABASE_USER"),
        "password": os.getenv("DATABASE_PASSWORD"),
        "driver_classpath": os.getenv("DATABASE_DRIVER"),
        "target_output_path": "./tmp/output/employee",
        "cols": [
            "BusinessEntityID", "NationalIDNumber", "OrganizationNode",
            "OrganizationLevel", "JobTitle", "BirthDate", "MaritalStatus",
            "Gender", "HireDate", "SalariedFlag", "VacationHours", "SickLeaveHours",
            "CurrentFlag", "ModifiedDate"
        ],
        "input_tables": [
            {"source": "mariadb", "database": os.getenv("DATABASE_NAME"), "table": "HumanResources_Employee"},
            {"source": "mariadb", "database": os.getenv("DATABASE_NAME"), "table": "HumanResources_EmployeePayHistory"}
        ]
    }

    spark = SparkSession.builder \
        .config("spark.jars", "jars/mariadb-java-client-3.3.0.jar") \
        .appName(configurations["appName"]) \
        .master(configurations["master"]) \
        .getOrCreate()

    jdbc_reader = MariaDbJDBCReader(
        spark,
        host=configurations["host"],
        port=configurations["port"],
        database=configurations["database"],
        user=configurations["user"],
        password=configurations["password"],
        driver=configurations["driver_classpath"]
    )

    job = JobExecutor(
        spark,
        jdbc_reader=jdbc_reader,
        cols=configurations["cols"],
        input_tables=configurations["input_tables"],
        target=configurations["target_output_path"]
    )
    job.execute()