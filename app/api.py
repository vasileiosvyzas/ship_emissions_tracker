import pandas as pd

from fastapi import FastAPI, Request
from http import HTTPStatus
from typing import Dict, List
from datetime import datetime
from functools import wraps

# Define application
app = FastAPI(
    title="Ship emissions tracker",
    description="Track emissions of ships larger than 5000 tonnes. Data are provided by the EU-MRV system.",
    version="0.1",
)

def construct_response(f):
    """Construct a JSON response for an endpoint."""

    @wraps(f)
    def wrap(request: Request, *args, **kwargs) -> Dict:
        results = f(request, *args, **kwargs)
        response = {
            "message": results["message"],
            "method": request.method,
            "status-code": results["status-code"],
            "timestamp": datetime.now().isoformat(),
            "url": request.url._url,
        }
        if "data" in results:
            response["data"] = results["data"]
        return response

    return wrap


@app.on_event("startup")
def load_artifacts():
    global df
    df = pd.read_csv('/Users/vasileiosvyzas/workspace/interview_exercises/CarbonChain/ship_emissions_tracker/data/interim/ship_emissions_tracker_2018_2021.csv')


@app.get("/", tags=['General'])
@construct_response
def _index(request: Request) -> Dict:
    """Health check."""
    response = {
        "message": HTTPStatus.OK.phrase,
        "status-code": HTTPStatus.OK,
        "data": {},
    }
    return response


@app.get("/ships", tags=['Ships'])
@construct_response
def _get_ships(request: Request) -> Dict:
    """
    Gets a list of unique ship types
    Returns:
        List: Returns a list of ships available in the dataset
    """
    
    print(df.shape)
    data = {'ships': list(df['Ship type'].unique())}
    
    response = {
        "message": HTTPStatus.OK.phrase,
        "status-code": HTTPStatus.OK,
        "data": data,
    }
    
    return response

@app.get("/emissions/{ship_type}", tags=['Ship type emissions'])
@construct_response
def _emissions_by_ship(request: Request, ship_type: str) -> Dict:
    data = df[df['Ship type'] == ship_type][['Total CO₂ emissions [m tonnes]', 'Reporting Period']].to_dict(orient='list')
    
    response = {
        "message": HTTPStatus.OK.phrase,
        "status-code": HTTPStatus.OK,
        "data": data,
    }
    
    return response


@app.get("/fuel_consumption/{ship_type}", tags=['Ship type fuel consumption'])
@construct_response
def _fuel_by_ship(request: Request, ship_type: str) -> Dict:
    data = df[df['Ship type'] == ship_type][['Total fuel consumption [m tonnes]', 'Reporting Period']].to_dict(orient='list')

    
    response = {
        "message": HTTPStatus.OK.phrase,
        "status-code": HTTPStatus.OK,
        "data": data,
    }
    
    return response


@app.get("/verifier_info", tags=["Info"])
@construct_response
def _get_verifier_info(request: Request) -> Dict:
    data = df[
        ['Verifier Name', 
         'Verifier NAB', 
         'Verifier Address', 
         'Verifier City', 
         'Verifier Accreditation number', 
         'Verifier Country']
        ].to_dict(orient='records')
    
    response = {
        "message": HTTPStatus.OK.phrase,
        "status-code": HTTPStatus.OK,
        "data": data,
    }
    
    return response
