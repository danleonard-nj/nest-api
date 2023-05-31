from typing import Dict
from framework.serialization import Serializable


class AuthorizationHeader(Serializable):
    def __init__(
        self,
        token: str
    ):
        self.bearer = token

    def to_dict(self):
        return {
            'Authorization': f'Bearer {self.bearer}'
        }


class SaveNestAuthCredentialRequest(Serializable):
    def __init__(
        self,
        data: Dict
    ):
        self.client_id = data.get('client_id')
        self.client_secret = data.get('client_secret')
        self.refresh_token = data.get('refresh_token')


class NestSensorDataRequest(Serializable):
    def __init__(
        self,
        data: Dict
    ):
        self.sensor_id = data.get('sensor_id')

        degrees_celsius = data.get('degrees_celsius', 0)
        humidity_percent = data.get('humidity_percent', 0)

        self.degrees_celsius = round(degrees_celsius, 2)
        self.humidity_percent = round(humidity_percent, 2)


class NestCommandRequest(Serializable):
    def __init__(
        self,
        data: Dict
    ):
        self.command_type = data.get('command_type')
        self.params = data.get('params')
