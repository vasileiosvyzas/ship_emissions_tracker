# ship_emissions_tracker
A simple REST API to expose endpoints for the CO2 emissions reports from the MRV system

Data from 2018 to 2021 are available here: https://mrv.emsa.europa.eu/#public/emission-report

Python version: 3.9

The API can be accessed here: https://emissions-tracking-app-gzv69.ondigitalocean.app/ship-emissions-tracker2

# Available endpoints
| Endpoints    | Description |
| -------- | ------- |
| /ships  | Gets a list of unique ship types Returns: List: Returns a list of ship types available in the dataset    |
| /ships/{ship_id} | The ship id is the IMO Number which is a unique identifier for a vessel. This function returns all the data related to a vessel for all the reporting years Returns: Dict: Returns a dictionary in the form dict like {‘index’ -> [index], ‘columns’ -> [columns], ‘data’ -> [values]} I thought this way will be easier to convert this dictionary back to a Pandas DataFrame     |
| /emissions/{ship_type}    | Takes a ship type as parameter and returns all the emissions created by it    |
| /fuel_consumption/{ship_type}   | returns the fuel consumption and reporting period for all the vessels belonging to a specific ship category    |
| /verifier_info    | Gives all the verifier info for all the vessels across all periods    |
| /technical_efficiency/{ship_id}    | returns the technical efficiency type and value for a specific ship    |


# How to run locally
## Virtual environment
    conda create -n env_name python=3.9
    conda activate env_name
    python3 -m pip install -e
    uvicorn app.api:app --host 0.0.0.0 --port 8000

The API should be accessible at http://0.0.0.0:8000/docs

## If Docker installed locally
    docker build -t ship_emissions .
    docker run -p 80:80 ship_emissions

The API should be accessible at http://0.0.0.0:80/docs
