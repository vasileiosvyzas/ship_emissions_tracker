from glob import glob
from typing import List

import pandas as pd


def read_datasets_and_merge(files: List[str]) -> pd.DataFrame:
    """Gets the list of files and merges them together

    Args:
        files (List[str]):

    Returns:
        pd.DataFrame: _description_
    """
    df_list = (pd.read_excel(file, header=2) for file in files)
    df = pd.concat(df_list, ignore_index=True)

    return df


def remove_null_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Gets the dataset, keeping the columns we want

    Args:
        df (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    df = df.loc[:, df.isnull().sum() == 0]

    return df


def process_technical_efficiency_values(df: pd.DataFrame) -> pd.DataFrame:
    """Gets the datasets, processes the technical efficiency column and splits it into two columns
    one column for the type and one column for the value

    Args:
        df (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """

    for index, row in df.iterrows():
        efficiency = row["Technical efficiency"]

        type = efficiency.split()[0]
        value = efficiency.split()[1].removeprefix("(")

        df.loc[index, "type"] = type
        df.loc[index, "gCO₂/t·nm"] = value

    return df


def main():
    raw_path = "../data/raw/"
    files = glob.glob(raw_path + "/*.xlsx")

    df = read_datasets_and_merge(files=files)
    df = remove_null_columns(df=df)
    df = process_technical_efficiency_values(df=df)
