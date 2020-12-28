from typing import Any, Dict, NamedTuple
import json


class ErrorType(NamedTuple):
    message: str
    data: Dict[str, Any]
    status: int = 500


class FleetGoException(Exception):
    def __init__(self, error: ErrorType):
        self.error = error

    def __str__(self):
        return repr(f'__ERROR_FLEET_GO__: {self.error.message}, STATUS: {self.error.status})')

    def to_json(self) -> Dict[str, Any]:

        return {
            'message': self.error.message,
            'data': json.dumps(self.error.data),
            'status': self.error.status
        }


class FleetGoWarning(FleetGoException):
    def __init__(self, error: ErrorType):
        self.error = error

    def __str__(self):
        return repr(f'__Warning_FLEET_GO__: {self.error.message}, STATUS: {self.error.status})')

    def to_json(self) -> Dict[str, Any]:

        return {
            'message': self.error.message,
            'data': json.dumps(self.error.data),
            'status': self.error.status
        }
