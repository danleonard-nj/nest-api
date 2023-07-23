from typing import Dict, Union

from framework.serialization import Serializable

from domain.nest import NestCommandType
from utils.helpers import parse


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


class AuthorizationRequest(Serializable):
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        grant_type='refresh_token',
        **kwargs
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.grant_type = grant_type

        self.__dict__.update(kwargs)


class NestSensorDataRequest(Serializable):
    def __init__(
        self,
        data: Dict
    ):
        self.sensor_id = data.get('sensor_id')

        degrees_celsius = data.get('degrees_celsius', 0)
        humidity_percent = data.get('humidity_percent', 0)

        self.degrees_celsius = (
            round(degrees_celsius, 2)
            if degrees_celsius != 0
            else 0
        )

        self.humidity_percent = (
            round(humidity_percent, 2)
            if humidity_percent != 0
            else 0
        )

        self.diagnostics = data.get('diagnostics')


class NestCommandClientRequest(Serializable):
    def __init__(
        self,
        command: str,
        **kwargs: Dict
    ):
        self.command = command
        self.kwargs = kwargs

    def to_dict(self) -> Dict:
        return {
            'command': self.command,
            'params': self.kwargs
        }


class NestCommandRequest(Serializable):
    def __init__(
        self,
        data: Dict
    ):
        self.command_type = data.get('command_type')
        self.params = data.get('params')


class NestSensorLogRequest(Serializable):
    def __init__(
        self,
        data: Dict
    ):
        self.device_id = data.get('device_id')
        self.log_level = data.get('log_level')
        self.message = data.get('message')


class NestCommandHandlerResponse(Serializable):
    def __init__(
        self,
        command_type: Union[str, NestCommandType],
        params: Dict,
        status: str
    ):
        self.command_type = parse(
            value=command_type,
            enum_type=NestCommandType)

        self.params = params
        self.status = status


class SensorDataPurgeResponse(Serializable):
    def __init__(
        self,
        deleted
    ):
        self.deleted = deleted
