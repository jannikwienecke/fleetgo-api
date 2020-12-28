from typing import Any, Dict, List, NamedTuple, Union
from loguru import logger
from .Fleet_Go import FLEET_GO_ENDPOINTS, FleetGo

import pandas as pd


def info(message: Any):
    logger.info(message)


class Employee(NamedTuple):
    FirstName: str
    Name: str


class FetchInputObject(NamedTuple):
    from_: str
    to: str
    employee: Employee
    extendedInfo: bool = False
    employeeId: Union[int, None] = None


COLUMNS_DF_TRIP: List[str] = [
    'drivernameid_id',
    'inoid_id',
    'starttime',
    'positiontextstart',
    'mileagestart',
    'endtime',
    'positiontextend',
    'positionstartlat',
    'positionstartlon',
    'positionendlat',
    'positionendlon',
    'truckweight',
]


class FleetGoTrip(FleetGo):

    """
    Class Responsible for fetching the TripData
    and for transforming them to a 'standarized' format
    Extends the FleetGo base class
    """

    def __init__(self, access_token: str):
        super().__init__()

        self.access_token = access_token

    def _get_employee_id(self, employee: Employee) -> int:

        def _get_employee():
            nonlocal df_employee
            df_employee = self._get_request(
                FLEET_GO_ENDPOINTS.EMPLOYEE)

        def _filter_employee():
            nonlocal df_employee

            filter_lastname = df_employee['Name'] == employee.Name
            filter_firstname = df_employee['FirstName'] == employee.FirstName
            df_employee = df_employee[filter_firstname & filter_lastname]

        def _validate_employee() -> pd.DataFrame:
            if len(df_employee) == 1:
                return df_employee

            elif len(df_employee) == 0:
                message = 'No Employee with this Name'

            else:
                message = 'More than one Employee with this name'

            self._raise(message,
                        data={'employee': employee})

        def _get_id() -> int:
            return df_employee.iloc[0]['Id']

        df_employee: pd.DataFrame

        _get_employee()
        _filter_employee()
        _validate_employee()

        return _get_id()

        return df_employee.iloc[0].to_json()

    def fetch_user_trip(self, input: FetchInputObject):

        def _parse_params():
            params = input._asdict()
            params['from'] = params.pop('from_')
            params.pop('employee')
            params['employeeId'] = employee_id

            return params

        def _validate_trip_response(df_trip: pd.DataFrame):
            if len(df_trip) == 0:
                warning_msg = 'Keine Tripdaten vorhanden FleetGo'
                self._raise(warning_msg, params, is_warning=True)

        def _parse_trip_response(df_trip: pd.DataFrame) -> pd.DataFrame:
            """
            Parse to same format as the fleetboard response is parsed
            """

            def _parse_time_column(row: Dict[str, Any]) -> pd.Timestamp:
                time_string: str = row['DateTime']
                datetime_with_tz: pd.Timestamp = pd.to_datetime(time_string)

                datetime_without_tz = pd.to_datetime(
                    str(datetime_with_tz)[:19])
                # datetime_without_tz: pd.Timestamp = pd.to_datetime(
                #     str(datetime_with_tz - datetime_with_tz.tz._offset)[:19])

                # return datetime_without_tz
                return datetime_without_tz

            df_trip = df_trip.rename(columns={
                "DriverId": "drivernameid_id",
                "EquipmentId": "inoid_id",
            })

            df_trip['starttime'] = df_trip.From.apply(
                _parse_time_column)

            df_trip['endtime'] = df_trip.To.apply(
                _parse_time_column)

            df_trip['positiontextstart'] = df_trip.From.apply(
                lambda x: x['Address']['City'])
            df_trip['positiontextend'] = df_trip.To.apply(
                lambda x: x['Address']['City'])

            df_trip['positionstartlat'] = df_trip.From.apply(
                lambda x: x['Location']['Latitude'])
            df_trip['positionstartlon'] = df_trip.From.apply(
                lambda x: x['Location']['Longitude'])

            df_trip['positionendlat'] = df_trip.To.apply(
                lambda x: x['Location']['Latitude'])
            df_trip['positionendlon'] = df_trip.To.apply(
                lambda x: x['Location']['Longitude'])

            df_trip['truckweight'] = 0
            df_trip['mileagestart'] = 0
            df_trip['mileageend'] = 0

            df_trip = df_trip[COLUMNS_DF_TRIP]

            df_trip.sort_values(
                by=["starttime", 'drivernameid_id'], inplace=True)

            return df_trip

        info(f"Get Employee id based on employee name: {input.employee}")
        employee_id = self._get_employee_id(input.employee)

        params = _parse_params()

        info(f'Fetch Trip Data: Params={params}')
        df_trip = self._get_request(FLEET_GO_ENDPOINTS.EMPLOYEE_TRIP,
                                    query_params=params)

        info("Validate Trip Response")
        _validate_trip_response(df_trip)

        info("Parse Trip Data")
        df_trip = _parse_trip_response(df_trip)

        info("Success Getting Trip Data")
        return df_trip
