import json
import sys

sys.path.append("app")
from api import app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

client = TestClient(app)


def test_index():
    """Testing the returns of the index endpoint"""
    response = json.loads(client.get("/").text)

    assert response["status-code"] == 200
    assert response["message"] == "OK"


def test_ship_types_returns_list():
    """Testing if the function returns correctly a list"""

    with client as cl:
        response = json.loads(cl.get("/ships").text)

        assert response["status-code"] == 200
        assert type(response["data"]["ships"]) == list


def test_wrong_ship_id_is_caught():
    """Testing if the ship_id provided in the /ships/{ship_id} endpoint exists in the data"""

    with client as cl:
        response = cl.get("/ships/23423")

        assert response.status_code == 404
        assert response.json() == {"detail": "The argument provided is not correct"}


def test_correct_response_with_correct_ship_id():
    with client as cl:
        response = json.loads(cl.get("/ships/6602898").text)

        assert response["status-code"] == 200
        assert response["message"] == "OK"
        assert len(response["data"]["data"]) != 0


def test_emissions_with_incorrect_ship_type():
    """Testing the response when user is requesting the emissions with an incorrect ship type label"""

    with client as cl:
        response = cl.get("/emissions/Passenger Ship")

        assert response.status_code == 404
        assert response.json() == {"detail": "The argument provided is not correct"}


def test_emissions_with_the_correct_ship_type():
    """Testing the response when user is requesting the emissions with the correct ship type label"""

    with client as cl:
        response = json.loads(cl.get("/emissions/Oil tanker").text)

        assert response["status-code"] == 200
        assert response["message"] == "OK"
        assert response["data"]["columns"] == [
            "Total CO₂ emissions [m tonnes]",
            "Reporting Period",
            "IMO Number",
        ]
        assert len(response["data"]["data"]) != 0


def test_fuel_consumption_with_incorrect_ship_type():
    """Testing the response when user is requesting the fuel consumption info with an incorrect ship type label"""

    with client as cl:
        response = cl.get("/fuel_consumption/Passenger Ship")

        assert response.status_code == 404
        assert response.json() == {"detail": "The argument provided is not correct"}


def test_fuel_consumption_with_the_correct_ship_type():
    """Testing the response when user is requesting the emissions with the correct ship type label"""

    with client as cl:
        response = json.loads(cl.get("/fuel_consumption/Oil tanker").text)

        assert response["status-code"] == 200
        assert response["message"] == "OK"
        assert response["data"]["columns"] == [
            "Total fuel consumption [m tonnes]",
            "Reporting Period",
            "IMO Number",
        ]
        assert len(response["data"]["data"]) != 0


def test_wrong_ship_id_requesting_technical_efficiency_info():
    """Testing if the ship_id provided in the /technical_efficiency/{ship_id} endpoint exists in the data"""

    with client as cl:
        response = cl.get("/technical_efficiency/23423")

        assert response.status_code == 404
        assert response.json() == {"detail": "The argument provided is not correct"}


def test_technical_efficiency_info_with_correct_ship_id():
    """Testing if the ship_id provided in the /technical_efficiency/{ship_id} endpoint exists in the data"""

    with client as cl:
        response = json.loads(cl.get("/technical_efficiency/9152820").text)

        assert response["status-code"] == 200
        assert response["message"] == "OK"
        assert response["data"] == {"type": "EIV", "gCO₂/t·nm": "15.97"}
