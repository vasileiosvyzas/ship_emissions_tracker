import sys
from http import HTTPStatus
from typing import Dict

import pandas as pd
from fastapi import FastAPI, HTTPException, Request

sys.path.append("src")
from api_decorators import construct_response  # noqa: E402

# Define application
app = FastAPI(
    title="Ship emissions tracker",
    description="Track emissions of ships larger than 5000 tonnes. Data are provided by the EU-MRV system.",
    version="0.1",
)


@app.on_event("startup")
def load_artifacts():
    """Read the csv with all the ship data from all the years we have downloaded"""
    global df
    df = pd.read_csv("data/interim/ship_emissions_tracker_2018_2021.csv")


@app.get("/", tags=["General"])
@construct_response
def _index(request: Request) -> Dict:
    """Health check."""
    response = {
        "message": HTTPStatus.OK.phrase,
        "status-code": HTTPStatus.OK,
        "data": {},
    }
    return response


@app.get("/ships", tags=["Ship Types"])
@construct_response
def _get_ships(request: Request) -> Dict:
    """
    Gets a list of unique ship types
    Returns:
        List: Returns a list of ship types available in the dataset
    """

    data = {"ships": list(df["Ship type"].unique())}

    response = {
        "message": HTTPStatus.OK.phrase,
        "status-code": HTTPStatus.OK,
        "data": data,
    }

    return response


@app.get("/ships/{ship_id}", tags=["Ship specific data"])
@construct_response
def _get_ship_data(request: Request, ship_id: int) -> Dict:
    """
    The ship id is the IMO Number which is a unique identifier for a vessel. This function returns all the
    data related to a vessel for all the reporting years
    Returns:
        Dict: Returns a dictionary in the form dict like {‘index’ -> [index], ‘columns’ -> [columns], ‘data’ -> [values]}
        I thought this way will be easier to convert this dictionary back to a Pandas DataFrame
    """

    if ship_id in df["IMO Number"].unique():
        data = df[df["IMO Number"] == ship_id].fillna("Missing").to_dict(orient="split")

        response = {
            "message": HTTPStatus.OK.phrase,
            "status-code": HTTPStatus.OK,
            "data": data,
        }

        return response
    else:
        raise HTTPException(status_code=404, detail="The argument provided is not correct")


@app.get("/emissions/{ship_type}", tags=["Ship type emissions"])
@construct_response
def _emissions_by_ship(request: Request, ship_type: str) -> Dict:
    """
    This function takes a ship type as parameter and returns all the emissions created by it.
    Args:
        request (Request): object that gives access to the request method and url
        ship_type (str): the ship type like Bulk carrier, Container ship etc.

    Returns:
        Dict: Returns a dictionary in the form dict like {‘index’ -> [index], ‘columns’ -> [columns], ‘data’ -> [values]}
        the columns are the total CO2 emissions, the reporting period and the id of the vessel
    """

    if ship_type in df["Ship type"].unique():
        data = df[df["Ship type"] == ship_type][
            ["Total CO₂ emissions [m tonnes]", "Reporting Period", "IMO Number"]
        ].to_dict(orient="split")

        response = {
            "message": HTTPStatus.OK.phrase,
            "status-code": HTTPStatus.OK,
            "data": data,
        }

        return response
    else:
        raise HTTPException(status_code=404, detail="The argument provided is not correct")


@app.get("/fuel_consumption/{ship_type}", tags=["Ship type fuel consumption"])
@construct_response
def _fuel_by_ship(request: Request, ship_type: str) -> Dict:
    """Function returns the fuel consumption and reporting period for all the vessels belonging to a specific category

    Args:
        request (Request): object that gives access to the request method and url
        ship_type (str): the ship type like Bulk carrier, Container ship etc.

    Returns:
        Dict: Returns a dictionary in the form dict like {‘index’ -> [index], ‘columns’ -> [columns], ‘data’ -> [values]}
        the columns are the total CO2 emissions, the reporting period and the id of the vessel
    """

    if ship_type in df["Ship type"].unique():
        data = df[df["Ship type"] == ship_type][
            ["Total fuel consumption [m tonnes]", "Reporting Period", "IMO Number"]
        ].to_dict(orient="split")

        response = {
            "message": HTTPStatus.OK.phrase,
            "status-code": HTTPStatus.OK,
            "data": data,
        }

        return response
    else:
        raise HTTPException(status_code=404, detail="The argument provided is not correct")


@app.get("/verifier_info", tags=["Info"])
@construct_response
def _get_verifier_info(request: Request) -> Dict:
    """Returns all the verifier info for all the vessels across all periods

    Args:
        request (Request): object that gives access to the request method and url

    Returns:
        Dict: Returns a dictionary in the form dict like {‘index’ -> [index], ‘columns’ -> [columns], ‘data’ -> [values]}
        the columns are the verifier name, NAB, address, city, accreditation number and country
    """
    data = df[
        [
            "Verifier Name",
            "Verifier NAB",
            "Verifier Address",
            "Verifier City",
            "Verifier Accreditation number",
            "Verifier Country",
        ]
    ].to_dict(orient="split")

    response = {
        "message": HTTPStatus.OK.phrase,
        "status-code": HTTPStatus.OK,
        "data": data,
    }

    return response


@app.get("/technical_efficiency/{ship_id}", tags=["Technical efficiency of a vessel"])
@construct_response
def _efficiency_by_ship(request: Request, ship_id: int) -> Dict:
    """Function returns the technical efficiency type and value for a specific ship

    Args:
        request (Request): object that gives access to the request method and url
        ship_id (int): the id of a ship

    Returns:
        Dict: Returns a dictionary with the type and gCO₂/t·nm values
    """

    if ship_id in df["IMO Number"].unique():
        val = df.loc[df[df["IMO Number"] == ship_id].index[0], "Technical efficiency"]
        data = {"type": val.split()[0], "gCO₂/t·nm": val.split()[1].removeprefix("(")}

        response = {
            "message": HTTPStatus.OK.phrase,
            "status-code": HTTPStatus.OK,
            "data": data,
        }

        return response
    else:
        raise HTTPException(status_code=404, detail="The argument provided is not correct")
