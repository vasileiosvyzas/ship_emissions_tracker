import json
import logging
import sys
import urllib.parse

import boto3
import pandas as pd
import pymysql

s3 = boto3.client("s3")

# rds settings
user_name = ""
password = ""
rds_host = ""
db_name = ""

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    conn = pymysql.connect(
        host=rds_host, user=user_name, passwd=password, db=db_name, connect_timeout=5
    )
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")


def lambda_handler(event, context):
    """
    This function creates a new RDS database table and writes records to it
    """
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")

    item_count = 0
    try:
        response = s3.get_object(Bucket=bucket, Key=key)

        df = pd.read_csv(response["Body"])
        df_small = df[
            [
                "IMO Number",
                "Ship type",
                "Total CO₂ emissions [m tonnes]",
                "Reporting Period",
                "Total fuel consumption [m tonnes]",
            ]
        ]
        df_small = df_small.head(1000)
        print("Bulk adding to the table")
        with conn.cursor() as cur:
            for row in df_small.values:
                row_tuple = tuple(row)
                sql_string = f"INSERT INTO emissions(IMONumber, ShipType, TotalCO₂Emissions, ReportingPeriod, TotalFuelConsumption) VALUES ({row_tuple[0]}, '{row_tuple[1]}', '{row_tuple[2]}', '{row_tuple[3]}', '{row_tuple[4]}');"
                cur.execute(sql_string)
                conn.commit()

            cur.execute("select * from emissions")
            logger.info("The following items have been added to the database:")
            for row in cur:
                item_count += 1
                logger.info(row)
        conn.commit()

        return "Added %d items to RDS MySQL table" % (item_count)

    except Exception as e:
        print(e)
        print(
            "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                key, bucket
            )
        )
        raise e
