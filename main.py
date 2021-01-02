from module.FleetGoVehicle import FleetGoVehicle
from typing import Any, Dict

from module.FleetGoTrip import Employee, FetchInputObject, FleetGoTrip
from module.FleetGoLogin import FleetGoLogin
from module.FleetGoDriver import FleetGoDriver
import sys
from module.Fleet_Go_Exceptions import ErrorType, FleetGoException, FleetGoWarning
import pandas as pd

from loguru import logger

logger.add(sys.stderr, format="{time} {level} {message}",
           filter="my_module", level="INFO", colorize=True)

logger.add("./logs-fleetgo/main.log", rotation='1 day')


class RunFleetGo():

    def __init__(self, user_input: FetchInputObject = None) -> None:
        self.user_input: FetchInputObject = user_input
        self.access_token = ''
        self.df_trip: pd.DataFrame
        self.df_driver: pd.DataFrame

    def __call__(self) -> pd.DataFrame:
        self.validate()
        self.login()
        self.fetch_trip()

        return self.df_trip

    def _raise(self, message: str, data: Dict[str, Any] = {}, status: int = 500):
        raise FleetGoException(error=ErrorType(
            message=message, data=data, status=status))

    def validate(self) -> None:

        def _validate_employee():

            if not isinstance(employee, Employee) or not (employee.Name and employee.FirstName):
                self._raise("Invalid Employee", data={'employee': employee})

        def _validate_from_to_datetimes():
            is_error = ''
            try:
                pd.to_datetime(from_)
            except Exception as error:
                is_error = f'from_: {error}'

            try:
                pd.to_datetime(to)
            except Exception as error:
                is_error = f'to: {error}'

            if is_error:
                self._raise(f"Invalid Date Parameter '{is_error}'", data={
                    'from_': from_,
                    'to': to,
                })

        employee = self.user_input.employee
        from_ = self.user_input.from_
        to = self.user_input.to

        _validate_employee()
        _validate_from_to_datetimes()

    def login(self) -> None:
        """Set the access_token"""
        fleet_login = FleetGoLogin()

        try:
            fleet_login.login()
        except FleetGoException as error:
            print(error)

        self.access_token = fleet_login.access_token

    def fetch_trip(self):
        """Fetch Driver Trip Data from FleetGo Api"""

        fleet_trip = FleetGoTrip(self.access_token)

        self.df_trip = fleet_trip.fetch_user_trip(self.user_input)

    def fetch_driver(self):
        """Fetch List of Driver from Fleetgo API"""

        fleet_driver = FleetGoDriver(self.access_token)

        df_driver = fleet_driver.fetch_driver()

        return df_driver

    def fetch_vehicle(self):
        """Fetch List of Vehicles from Fleetgo API"""

        fleet_vehicle = FleetGoVehicle(self.access_token)

        df_vehicle = fleet_vehicle.fetch_vehicle()

        return df_vehicle


def fetch_trip_data_fleetgo(user_input: FetchInputObject):

    logger.info("RUN fetch_trip_data_fleetgo")

    try:
        df_trip = RunFleetGo(user_input)()
        logger.info(f'Result: Dataframe with Length: {len(df_trip)} ')

    except FleetGoWarning as fleetgo_warning:
        logger.warning(fleetgo_warning)

    except FleetGoException as fleego_exception:
        logger.error(fleego_exception)


def fetch_driver_fleetgo():

    logger.info("fetch_driver_fleetgo...")

    try:
        fleet_go = RunFleetGo()
        fleet_go.login()
        df_driver = fleet_go.fetch_driver()
        logger.info(f'Result: Dataframe with Length: {len(df_driver)} ')
        return df_driver

    except FleetGoWarning as fleetgo_warning:
        logger.warning(fleetgo_warning)
        return fleetgo_warning

    except FleetGoException as fleego_exception:
        logger.error(fleego_exception)
        return fleego_exception


def fetch_vehicle_fleetgo():

    logger.info("fetch_vehicle_fleetgo...")

    try:
        fleet_go = RunFleetGo()
        fleet_go.login()
        df_vehicle = fleet_go.fetch_vehicle()
        logger.info(f'Result: Dataframe with Length: {len(df_vehicle)} ')
        return df_vehicle

    except FleetGoWarning as fleetgo_warning:
        logger.warning(fleetgo_warning)
        return fleetgo_warning

    except FleetGoException as fleego_exception:
        logger.error(fleego_exception)
        return fleego_exception


if __name__ == '__main__':

    employee = Employee(FirstName='Jannik', Name='Wienecke')
    # employee = Employee(FirstName='Kamil', Name='Kaczorowski')

    input_ = FetchInputObject(
        from_='2020-11-22T00:00:00',
        to='2020-12-15T00:00:00',
        employee=employee)

    # fetch_trip_data_fleetgo(input_)
    # fetch_driver_fleetgo()
    fetch_vehicle_fleetgo()
