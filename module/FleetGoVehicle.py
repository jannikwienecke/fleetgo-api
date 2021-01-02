from typing import Any

import pandas as pd
from loguru import logger

from .Fleet_Go import FLEET_GO_ENDPOINTS, FleetGo


def info(message: Any):
    logger.info(message)


COLUMNS_DF_DRIVER = [
    'id',
    'inoid',
    'registration',
    'source'
]


class FleetGoVehicle(FleetGo):

    def __init__(self, access_token: str):
        super().__init__()
        self.access_token = access_token
        self.df_vehicle: pd.DataFrame = pd.DataFrame([])

    def fetch_vehicle(self) -> pd.DataFrame:

        def _fetch_all():
            query_params = {'groupId': 0, 'hasDeviceOnly': False}
            self.df_vehicle = self._get_request(
                FLEET_GO_ENDPOINTS.VEHICLE, query_params=query_params)

        def _parse_df():
            """Parse to same format as fleetboard"""

            self.df_vehicle['id'] = self.df_vehicle['Id']
            self.df_vehicle['inoid'] = self.df_vehicle['Id']

            self.df_vehicle['registration'] = self.df_vehicle.apply(
                lambda x: x['EquipmentHeader']['SerialNumber'], axis=1)

            self.df_vehicle['source'] = 'FleetGo'

            self.df_vehicle = self.df_vehicle[COLUMNS_DF_DRIVER]

        _fetch_all()
        _parse_df()

        return self.df_vehicle
