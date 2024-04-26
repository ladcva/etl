import os
import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from jobs.extract_employee import JobExecutor
from reader.jdbc import MariaDbJDBCReader

# Mock database configuration
DATABASE_CONFIG = {
    "appName": "PySpark Example - MariaDB Example",
    "master": "local",
    "database": os.getenv("DATABASE_NAME"),
    "host": os.getenv("DATABASE_HOST"),
    "port": 3306,
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

@pytest.fixture(scope="session")
def spark_session():
    """
    Fixture for creating a SparkSession.
    """
    # Create a SparkSession with local mode
    spark = SparkSession.builder \
        .appName("pytest-pyspark-example") \
        .master("local[2]") \
        .getOrCreate()
    
    # Once all tests using the fixture are done, stop the SparkSession
    yield spark
    spark.stop()

def test_job_executor_execute(spark_session, monkeypatch):

    assert 1 == 1

