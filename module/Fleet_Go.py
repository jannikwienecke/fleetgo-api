from .Fleet_Go_Exceptions import ErrorType, FleetGoException, FleetGoWarning
import json

from enum import Enum
from typing import Any, Dict, Mapping, Union

import requests
from loguru import logger
import pandas as pd


class FLEET_GO_ENDPOINTS(Enum):

    LOGIN = 'session/login'
    TOKEN = 'session/token'
    EMPLOYEE_TRIP = 'Trips/GetEmployeeTrips'
    EMPLOYEE = 'Employee/Get'
    VEHICLE = 'Equipment/GetFleet'


class FleetGo:

    """
        FleetGo Base Class
    """

    FLEET_GO_BASE_ENDPOINT = 'https://api.fleetgo.com/api/'

    def __init__(self, access_token: Union[str, None] = None) -> None:
        self.access_token = access_token

    def _post_request(self, endpoint: FLEET_GO_ENDPOINTS,
                      payload: Mapping[str, Any], **config: str):

        url = self.FLEET_GO_BASE_ENDPOINT + endpoint.value

        if not url:
            self._raise(message=f'Invalid Endpoint', data={'endpoint': url})

        headers = {'Content-Type': 'application/json'}

        response = requests.post(url, data=json.dumps(payload), headers={
                                 **headers, **config})

        return response.json()

    def _get_request(self, endpoint: FLEET_GO_ENDPOINTS,
                     query_params: Dict[str, str] = {},
                     ** config: str
                     ) -> pd.DataFrame:

        url = self.FLEET_GO_BASE_ENDPOINT + endpoint.value

        if not url:
            self._raise(message=f'Invalid Endpoint', data={'endpoint': url})

        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        logger.info(
            f"Request FleetGo: URL={url} PARAMS={query_params}")

        response = requests.get(url, params=query_params, headers={
            **headers, **config})

        logger.info(
            f"Response FleetGo: Ok={response.ok} Status={response.status_code}")

        if response.ok:
            return pd.DataFrame(response.json())
        else:
            response_error = response.json()
            print(response_error)
            error_msg = f'Could not fetch data from FleetGo: {response_error["ErrorDescription"]}'
            self._raise(error_msg, query_params)

    def _raise(self, message: str, data: Dict[str, Any] = {},
               status: int = 500, is_warning: bool = False):

        if is_warning:
            raise FleetGoWarning(error=ErrorType(
                message=message, data=data, status=status))
        else:
            raise FleetGoException(error=ErrorType(
                message=message, data=data, status=status))

        # code.interact(local=locals())
