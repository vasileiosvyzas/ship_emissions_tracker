import os

import awswrangler as wr
import boto3
import pandas as pd
from dotenv import find_dotenv, load_dotenv

import streamlit as st

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("AWD_SECRET_ACCESS_KEY")
TABLE_NAME = os.environ.get("TABLE_NAME")
DATABASE_NAME = os.environ.get("DATABASE_NAME")

print(DATABASE_NAME)
print(TABLE_NAME)

boto3.setup_default_session(
    region_name="us-east-1",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,
)

st.title("Ship emissions analysis")


@st.cache_data
def load_data():
    df = wr.athena.read_sql_query(f"SELECT * FROM {TABLE_NAME}", database=DATABASE_NAME)
    df["doc_expiry_date"] = pd.to_datetime(df["doc_expiry_date"], dayfirst=True)
    df["doc_issue_date"] = pd.to_datetime(df["doc_issue_date"], dayfirst=True)

    return df


df = load_data()

print(df["reporting_period"].head())
print(df.dtypes)

with st.expander("Data Preview"):
    st.dataframe(
        data=df, column_config={"reporting_period": st.column_config.NumberColumn(format="%d")}
    )

col1, col2 = st.columns([2, 3])

with col1:
    st.write("Total emissions through the years")
    st.bar_chart(data=df, x="reporting_period", y="total_co_emissions_m_tonnes_")

    st.write("Emissions by ship")
    st.bar_chart(data=df, x="ship_type", y="total_co_emissions_m_tonnes_")


with col2:
    df.loc[df["technical_efficiency_value"] == "", "technical_efficiency_value"] = "0"
    df["technical_efficiency_value"] = df["technical_efficiency_value"].fillna("0")
    df["technical_efficiency_value"] = df["technical_efficiency_value"].astype(float)

    efficiency_df = df.groupby("ship_type")["technical_efficiency_value"].mean().reset_index()
    st.write("Best and worst technical efficiency by ship type")
    st.bar_chart(data=efficiency_df, x="ship_type", y="technical_efficiency_value")

    df = df.rename(
        columns={
            "co_emissions_which_occurred_within_ports_under_a_ms_jurisdiction_at_berth_m_tonnes_": "emissions_within_ports_at_berth"
        }
    )
    st.write("Ship type emissions while at berth")
    st.bar_chart(data=df, x="ship_type", y="emissions_within_ports_at_berth")
