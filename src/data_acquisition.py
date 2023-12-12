import logging
import os
import time
import traceback

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import find_dotenv, load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("AWD_SECRET_ACCESS_KEY")

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(
    filename="ship_emissions_tracker/logs/logfile.log",
    level=logging.DEBUG,
    format=LOG_FORMAT,
    filemode="w",
)
logger = logging.getLogger()


def get_reporting_table_content():
    """
    This function gets the metadata from the table in the website that contains
    the links to the excel reports

    Returns: a Pandas dataframe with the columns: ['Reporting Period', 'Version', 'Generation Date', 'File']. It
    writes the dataframe on disk
    """
    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(service=service, options=options)

    try:
        logger.info("Visiting the Thetis MRV website")

        driver.get("https://mrv.emsa.europa.eu/#public/emission-report")
        time.sleep(30)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="exportablegrid-1137-body"]'))
        )

        tables = driver.find_element(By.XPATH, '//*[@id="exportablegrid-1137-body"]')
        elements = tables.find_elements(By.TAG_NAME, "table")

        reports = []
        keys = ["Reporting Period", "Version", "Generation Date", "File"]

        for table in elements:
            result_dict = {keys[i]: value for i, value in enumerate(table.text.split("\n")[1:])}
            reports.append(result_dict)

        logger.info("Got the table data of the reports")
        logger.info(pd.DataFrame(reports).head())

        return pd.DataFrame(reports)

    except Exception as e:
        logger.error(f"An error occurred while getting the data: {e}")
        logger.error(traceback.format_exc())
    finally:
        driver.quit()


def download_new_file(report, download_directory):
    """
    This function gets called to download the new file from the website
    It uses Selenium to click on the new link text

    """
    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_experimental_option("prefs", {"download.default_directory": download_directory})

    driver = webdriver.Chrome(service=service, options=options)
    driver.get("https://mrv.emsa.europa.eu/#public/emission-report")
    time.sleep(10)

    try:
        logger.info(f"Downloading the new report: {report}")

        wait = WebDriverWait(driver, 30)
        link = wait.until(EC.presence_of_element_located((By.LINK_TEXT, report)))
        link.click()

        time.sleep(10)

    except Exception as e:
        logger.error(f"An error occurred while getting the data: {e}")
        logger.error(traceback.format_exc())
    finally:
        driver.quit()


def compare_versions_and_download_file(current_df, new_df):
    """
    This function compares the version of the file from the new time we pull the data
    and it compares the new version with the old version that is on disk
    """
    logger.info("Comparing the versions in the current and new dataframes")

    merged_df = pd.merge(
        current_df, new_df, on="Reporting Period", how="outer", suffixes=("_current", "_new")
    )
    new_versions = merged_df[merged_df["Version_new"] > merged_df["Version_current"]]

    logger.info("New versions")
    logger.info(new_versions.head())

    if not new_versions.empty:
        logger.info(
            f"New version: {new_versions['Version_new']} was found for file: {new_versions['File_new']}"
        )

        download_directory = "/Users/vasileiosvyzas/workspace/interview_exercises/CarbonChain/ship_emissions_tracker/data/raw"
        for index, row in new_versions.iterrows():
            logger.info("Updating the current dataframe with the new versions")

            current_df.loc[
                current_df["Reporting Period"] == row["Reporting Period"], "Version"
            ] = row["Version_new"]
            current_df.loc[
                current_df["Reporting Period"] == row["Reporting Period"], "Generation Date"
            ] = row["Generation Date_new"]
            current_df.loc[current_df["Reporting Period"] == row["Reporting Period"], "File"] = row[
                "File_new"
            ]

            download_new_file(report=row["File_new"].strip(), download_directory=download_directory)

            filepath = f"{download_directory}/{row['File_new'].strip()}.xlsx"
            year = row["Reporting Period"]
            filename = f"{row['File_new'].strip()}.xlsx"

            upload_file(
                file_name=filepath,
                bucket="eu-marv-ship-emissions",
                object_name=f"raw/{year}/{filename}",
            )

            delete_file_from_local_directory(filepath=filepath)

            return current_df
    else:
        logger.info("There are no new versions of the data in the website")


def delete_file_from_local_directory(filepath):
    if os.path.exists(filepath):
        logger.info(f"Deleting the file from the local path: {filepath}")
        os.remove(filepath)
    else:
        logger.info("The file does not exist so I couldn't delete it")


def fix_column_types(df):
    logger.info("Convering the column types of the dataframe")

    df["Reporting Period"] = df[["Reporting Period"]].astype(int)
    df["Version"] = df[["Version"]].astype(int)
    df["Generation Date"] = pd.to_datetime(df["Generation Date"], dayfirst=True)

    return df


def upload_file(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
    )

    try:
        logger.info(
            f"Uploading the file: {file_name} to the bucket: {bucket} with object name: {object_name}"
        )

        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def main():
    logger.info("Getting the new metadata from the reports table")
    reports_df_new = get_reporting_table_content()
    reports_df_new = fix_column_types(reports_df_new)

    logger.info("New metadata from the website")
    logger.info(reports_df_new.head())

    reports_df_old = pd.read_csv("ship_emissions_tracker/data/raw/reports_metadata.csv")
    reports_df_old = fix_column_types(reports_df_old)

    logger.info("Current metadata from local master file")
    logger.info(reports_df_old.head())

    reports_df_updated = compare_versions_and_download_file(
        current_df=reports_df_old, new_df=reports_df_new
    )

    logger.info("Got new files and added them to the S3 bucket")

    reports_df_updated.to_csv("ship_emissions_tracker/data/raw/reports_metadata.csv", index=False)


if __name__ == "__main__":
    main()
