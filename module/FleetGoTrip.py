from typing import Any, Dict, List, NamedTuple, Union
from loguru import logger
from pandas.core.arrays import integer
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

        def _fetch():

            def _run_get_request():
                return self._get_request(FLEET_GO_ENDPOINTS.EMPLOYEE_TRIP,
                                         query_params=params)

            def _get_time_range():
                time_range: int = (to - from_).days
                return time_range

            def _get_number_requests_needed():
                return int(time_range_days / (MAX_TIME_RANGE - 1)) + 1

            def _split_time_range_and_run_requests():

                def _set_params_from_to_datet():
                    from_date: pd.Timestamp = from_ + \
                        pd.to_timedelta(6, unit='days') * request_counter

                    to_date: pd.Timestamp = from_date + \
                        pd.to_timedelta(6, unit='days')

                    if to_date > to:
                        to_date = to

                    params['from'] = from_date
                    params['to'] = to_date

                def _merge_dataframes():
                    df_all: pd.DataFrame = pd.concat(df_trip_all)

                    df_all_no_duplicates = df_all.drop_duplicates('Timestamp')

                    return df_all_no_duplicates

                def _loop_requests():
                    nonlocal request_counter
                    for request_counter in range(number_requests_needed):
                        _set_params_from_to_datet()
                        df_trip_partial = _run_get_request()
                        info(f"LEN PARTIAL {len(df_trip_partial)}")
                        df_trip_all.append(df_trip_partial)

                request_counter = 0
                df_trip_all: List[pd.DataFrame] = []

                _loop_requests()
                df = _merge_dataframes()

                return df

            MAX_TIME_RANGE = 7

            from_: pd.Timestamp = pd.to_datetime(params['from'])

            to: pd.Timestamp = pd.to_datetime(params['to'])

            time_range_days: int = _get_time_range()

            number_requests_needed: int = _get_number_requests_needed()

            time_range_to_long = time_range_days > MAX_TIME_RANGE

            if (time_range_to_long):
                return _split_time_range_and_run_requests()
            else:
                return _run_get_request()

        info(f"Get Employee id based on employee name: {input.employee}")
        employee_id = self._get_employee_id(input.employee)

        params = _parse_params()

        df_trip = _fetch()

        info("Validate Trip Response")
        _validate_trip_response(df_trip)

        info("Parse Trip Data")
        df_trip = _parse_trip_response(df_trip)

        info("Success Getting Trip Data")
        return df_trip
