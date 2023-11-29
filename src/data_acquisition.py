import time
import traceback

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait


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
        driver.get("https://mrv.emsa.europa.eu/#public/emission-report")

        time.sleep(60)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="exportablegrid-1137-body"]'))
        )

        tables = driver.find_element(By.XPATH, '//*[@id="exportablegrid-1137-body"]')
        elements = tables.find_elements(By.TAG_NAME, "table")

        keys = ["Reporting Period", "Version", "Generation Date", "File"]
        reports = []
        for table in elements:
            print(table.text)
            result_dict = {keys[i]: value for i, value in enumerate(table.text.split("\n")[1:])}
            reports.append(result_dict)
            print()

        return pd.DataFrame(reports)

    except Exception as e:
        print(f"An error occurred: {e}")
        print(traceback.format_exc())
    finally:
        driver.quit()


def download_new_file(report):
    """
    This function gets called to download the new file from the website
    It uses Selenium to click on the new link text

    """
    print("Downloading the new file")

    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")

    # Specify the download directory
    download_directory = "/Users/vasileiosvyzas/workspace/interview_exercises/CarbonChain/ship_emissions_tracker/data/raw"
    options.add_experimental_option("prefs", {"download.default_directory": download_directory})

    driver = webdriver.Chrome(service=service, options=options)
    driver.get("https://mrv.emsa.europa.eu/#public/emission-report")
    time.sleep(10)

    try:
        print(f"===={report}=====")
        wait = WebDriverWait(driver, 30)
        link = wait.until(EC.presence_of_element_located((By.LINK_TEXT, report)))
        print("clicking the link")
        link.click()
        time.sleep(10)

    except Exception as e:
        print(f"An error occurred: {e}")
        print(traceback.format_exc())
    finally:
        driver.quit()


def compare_versions(current_df, new_df):
    """
    This function compares the version of the file from the new time we pull the data
    and it compares the new version with the old version that is on disk
    """
    print("Comparing the two versions")

    merged_df = pd.merge(
        current_df, new_df, on="Reporting Period", how="outer", suffixes=("_current", "_new")
    )
    new_versions = merged_df[merged_df["Version_new"] > merged_df["Version_current"]]
    print("== New versions ==")
    print(new_versions)

    for index, row in new_versions.iterrows():
        current_df.loc[current_df["Reporting Period"] == row["Reporting Period"], "Version"] = row[
            "Version_new"
        ]
        current_df.loc[
            current_df["Reporting Period"] == row["Reporting Period"], "Generation Date"
        ] = row["Generation Date_new"]
        current_df.loc[current_df["Reporting Period"] == row["Reporting Period"], "File"] = row[
            "File_new"
        ]

        print(row["File_new"])
        print(f"'{row['File_new']}'")
        download_new_file(row["File_new"].strip())

    print(current_df)
    return current_df


def fix_column_types(df):
    df["Reporting Period"] = df[["Reporting Period"]].astype(int)
    df["Version"] = df[["Version"]].astype(int)
    df["Generation Date"] = pd.to_datetime(df["Generation Date"], dayfirst=True)

    return df


def update_table_data():
    """
    This function updates the file on disk with the new version of the metadata
    For example, if there is a new version of a file, this function should store these data
    on the file
    """
    pass


def main():
    print("Getting the new metadata from the reports table")
    reports_df_new = get_reporting_table_content()
    reports_df_new = fix_column_types(reports_df_new)
    print(reports_df_new.head())

    reports_df_old = pd.read_csv("ship_emissions_tracker/data/raw/reports_metadata.csv")
    reports_df_old = fix_column_types(reports_df_old)
    print(reports_df_old.head())

    reports_df_updated = compare_versions(current_df=reports_df_old, new_df=reports_df_new)
    reports_df_updated.to_csv("ship_emissions_tracker/data/raw/reports_metadata.csv", index=False)


if __name__ == "__main__":
    main()
