from typing import Any

import pandas as pd
from loguru import logger

from .Fleet_Go import FLEET_GO_ENDPOINTS, FleetGo


def info(message: Any):
    logger.info(message)


COLUMNS_DF_DRIVER = [
    'id',
    'drivernameid',
    'lastname',
    'firstname',
    'source'
]


class FleetGoDriver(FleetGo):

    def __init__(self, access_token: str):
        super().__init__()
        self.access_token = access_token

    def fetch_driver(self) -> pd.DataFrame:

        def _fetch_all():
            nonlocal df_driver
            df_driver = self._get_request(
                FLEET_GO_ENDPOINTS.EMPLOYEE)

        def _parse_df():
            """Parse to same format as fleetboard"""
            nonlocal df_driver

            # info(f'df_driver = {df_driver.iloc[0]["User"]}')

            # df_driver['drivernameid'] = df_driver.apply(_get_driver_id, axis=1)

            df_driver['lastname'] = df_driver['Name']
            df_driver['firstname'] = df_driver['FirstName']
            df_driver['id'] = df_driver['Id']
            df_driver['drivernameid'] = df_driver['Id']
            df_driver['source'] = 'FleetGo'

            df_driver = df_driver[COLUMNS_DF_DRIVER]

        df_driver: pd.DataFrame = pd.DataFrame([])

        _fetch_all()
        _parse_df()

        return df_driver
