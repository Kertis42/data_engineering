import logging
from airflow.hooks.base_hook import BaseHook
from pyspark.sql import SparkSession


def s_function(value,*args, **kwargs):

    spark = SparkSession.builder \
            .config('spark.driver.extraClassPath', '/home/user/airflow/dags/postgresql-42.2.23.jar') \
            .master('local')\
            .appName("Dshop")\
            .getOrCreate()
    logging.info(f"Spark session created")
    df = spark.read.load(f"/bronze/postgres/{'{{ ds }}'}/{value}.csv"
                    , header="true"
                    , inferSchema="true"
                    , format="csv")
    logging.info(f"Reading {value}")

    df= df.dropDuplicates()
    logging.info(f"Duplicates removed from {value}")

    # write to silver
    df = df.write.parquet(f"/silver/postgres/{value}", mode='overwrite')
    logging.info(f"Writing table {value} to Silver")

    # write to greenplum
    df = spark.read.parquet(f"/silver/postgres/{value}")
    host = BaseHook.get_connection('greenplum_dshop').host
    port = BaseHook.get_connection('greenplum_dshop').port
    dbase = BaseHook.get_connection('greenplum_dshop').schema
    gp_url = f"jdbc:postgresql://{host}:{port}/{dbase}"
    gp_properties = {"user": BaseHook.get_connection('greenplum_dshop').login,
                     "password": BaseHook.get_connection('greenplum_dshop').password}
    df.write.jdbc(gp_url
                       , table=value
                       , properties=gp_properties
                       , mode='overwrite')
    logging.info(f"Writing {value} to Greenplum")
