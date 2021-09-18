import json
import logging
import requests
from pyspark.sql import SparkSession
from requests.exceptions import HTTPError
from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook
from function.config import stock_config


def stock(f_date, **kwargs):
    logging.info(f"Start to received access token")
    post_url = BaseHook.get_connection('stock_api_post').host
    post_headers = {'Content-Type': 'application/json'}
    post_data = {"username": BaseHook.get_connection('stock_api_post').login,
                 "password": BaseHook.get_connection('stock_api_post').password}
    post_req = requests.post(url=post_url, headers=post_headers, data=json.dumps(post_data))
    token = post_req.json()['access_token']
    logging.info(f"Access token successfully received")

    get_url = BaseHook.get_connection('stock_api_get').host
    get_headers = {'Content-Type': 'application/json', 'Authorization': "JWT " + token}
    get_data = {"date": f_date}

    try:
        get_req = requests.get(url=get_url, headers=get_headers, data=json.dumps(get_data))
    except HTTPError:
        print('Error!')

    logging.info(f"Data for {f_date} has been successfully received")

    hdfs_conn = BaseHook.get_connection('hdfs_conn_id')
    client = InsecureClient(hdfs_conn.host, user=hdfs_conn.login)
    logging.info(f"Connect to HDFS")

    with client.write(f'/bronze/api/{f_date}/stock/{f_date}.json', encoding='utf-8') as json_file:
        data = get_req.json()
        json.dump(data, json_file)
    logging.info(f"Json file successfully write")


def s_stock(f_date, **kwargs):
    spark = SparkSession.builder \
        .master('local') \
        .appName("Stock_to_silver") \
        .getOrCreate()
    logging.info(f"Spark session created")

    # Schema of fields
    schema = StructType([
        StructField("date", DateType(), False),
        StructField("product_id", IntegerType(), False)
    ])

    # read from bronze
    df = spark.read.schema(schema).json(f"/bronze/api/{f_date}/stock/{f_date}.json")
    logging.info(f"Reading {f_date}.json")

    df = df.dropDuplicates()
    logging.info(f"Duplicates removed from {f_date}.json")

    # write to silver
    df = df.write.parquet(f"/silver/stock/api_data", mode='append')
    logging.info(f"Writing parquet to Silver")


def g_stock(f_date, **kwargs):
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/airflow/dags/postgresql-42.2.23.jar') \
        .master('local') \
        .appName("Stock_to_gp") \
        .getOrCreate()
    logging.info(f"Spark session created")

    # read from silver
    df = spark.read.parquet(f"/silver/stock/api_data")
    logging.info(f"Reading data from silver")

    # write to greenplum
    host = BaseHook.get_connection('greenplum_dshop').host
    port = BaseHook.get_connection('greenplum_dshop').port
    dbase = BaseHook.get_connection('greenplum_dshop').schema
    gp_url = f"jdbc:postgresql://{host}:{port}/{dbase}"
    gp_properties = {"user": BaseHook.get_connection('greenplum_dshop').login,
                     "password": BaseHook.get_connection('greenplum_dshop').password}
    df.write.jdbc(gp_url
                  , table='fact_stock'
                  , properties=gp_properties
                  , mode='append')
    logging.info(f"Writing to Greenplum")
