from .Fleet_Go import FLEET_GO_ENDPOINTS, FleetGo
import threading
from loguru import logger
import os
import sched
import tempfile
import time
from typing import Any, Dict
from dotenv import load_dotenv
load_dotenv()


__FLEET_GO_USER_PASSWORD__ = os.environ['FLEET_GO_USER_PASSWORD']
__FLEET_GO_USERNAME__ = os.environ['FLEET_GO_USERNAME']
__FLEET_GO_CLIENT_SECRET__ = os.environ['FLEET_GO_CLIENT_SECRET']
__FLEET_GO_CLIEND_ID__ = os.environ['FLEET_GO_CLIEND_ID']

REFRESH_TOKEN = 'refresh_token'
ACCESS_TOEN = 'access_token'


def info(message: str):
    logger.info(message)


class FleetGoLogin(FleetGo):

    BASE_SESSION_DATA = {
        "client_id": __FLEET_GO_CLIEND_ID__,
        "client_secret": __FLEET_GO_CLIENT_SECRET__,
    }
    LOGIN_DATA = {
        **BASE_SESSION_DATA,
        "username": __FLEET_GO_USERNAME__,
        "password": __FLEET_GO_USER_PASSWORD__
    }

    VERIFY_CODE = 'lkdsmaofoiaffopjfpapm'

    TOKEN_DATA = {
        **BASE_SESSION_DATA,
        'code': VERIFY_CODE,
        'grant_type': REFRESH_TOKEN,
        REFRESH_TOKEN: ''
    }

    TOKEN_PATH = os.path.join(
        tempfile.gettempdir(),
        'fleet_go',
        ACCESS_TOEN,
    )

    REFRESH_TOKEN_PATH = os.path.join(
        tempfile.gettempdir(),
        'fleet_go',
        REFRESH_TOKEN,
    )

    TOKEN_MAX_AGE = 60 * 60  # 60 minutes

    def __init__(self):
        super().__init__()
        self.access_token = ''
        self.refresh_token = ''

    def _start_refresh_token_scheduler(self):

        def refresh_token(sc: Any):

            info('Refresh FleetGo Token...')

            payload = self.TOKEN_DATA
            payload[REFRESH_TOKEN] = self.refresh_token

            login_response = self._post_request(
                FLEET_GO_ENDPOINTS.TOKEN, payload=payload)

            info(f"Response Refresh Token: {login_response}")

            self._set_tokens(login_response)

            s.enter(refresh_intervall, 1, refresh_token, (sc,))

        info('Start Scheduler - Refresh Token every 30 Minutes.')

        # refresh_intervall = 10
        refresh_intervall = self.TOKEN_MAX_AGE / 2
        s = sched.scheduler(time.time, time.sleep)

        s.enter(refresh_intervall, 1, refresh_token, (s,))

        s.run()

    def _set_tokens(self, login_response: Dict[str, Any]):

        def _set():

            self.access_token = login_response[ACCESS_TOEN]
            self.refresh_token = login_response[REFRESH_TOKEN]

        def _write_to_file():

            try:
                with open(self.TOKEN_PATH, 'w') as out_file:
                    out_file.write(self.access_token)

                with open(self.REFRESH_TOKEN_PATH, 'w') as out_file:
                    out_file.write(self.refresh_token)

            except FileNotFoundError as error:
                msg = error.strerror
                data = {'Filename': error.filename}
                self._raise(message=msg, data=data)

        _set()
        _write_to_file()

    def login(self):

        def _set_token():
            with open(self.TOKEN_PATH, 'r') as in_file:
                info("Login:Get Token From File.")
                access_token = in_file.read()
                self.access_token = access_token

            with open(self.REFRESH_TOKEN_PATH, 'r') as in_file:
                info("Login:Get Refresh Token From File.")
                refresh_token = in_file.read()
                self.refresh_token = refresh_token

        def _set_token_from_file() -> bool:

            is_file_token = os.path.isfile(self.TOKEN_PATH)
            is_file_refresh_token = os.path.isfile(self.TOKEN_PATH)

            if not is_file_token or not is_file_refresh_token:
                return False

            token_age = time.time() - os.path.getmtime(self.TOKEN_PATH)
            is_token_expired = token_age > self.TOKEN_MAX_AGE
            if is_token_expired:
                return False

            _set_token()

            return True

        def _request_login():
            logger.info("Login:Request new Token")
            login_response: Dict[str, Any] = self._post_request(
                FLEET_GO_ENDPOINTS.LOGIN, self.LOGIN_DATA)

            return login_response

        if _set_token_from_file():
            pass
        else:
            info('No Valid Token - Need To Get One.')
            login_response = _request_login()
            self._set_tokens(login_response)

        thread_1 = threading.Thread(
            target=self._start_refresh_token_scheduler)

        thread_1.start()

        info("Login FleetGo Successfully")
