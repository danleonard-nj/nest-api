import enum
from typing import Dict, List, Union
from domain.enums import IntegrationEventType, IntergationDeviceType

from utils.helpers import parse


class DeviceIntegrationConfig:
    def __init__(
        self,
        sensor_id: str,
        integrations: List[Dict]
    ):
        self.sensor_id = sensor_id
        self.integrations = integrations

    @staticmethod
    def from_json_object(
        data: Dict
    ):
        return DeviceIntegrationConfig(
            sensor_id=data.get('sensor_id'),
            integrations=data.get('integrations'))

    def is_supported(
        self,
        integration_type: Union[IntergationDeviceType, str]
    ):
        for integration in self.integrations:
            device_type = integration.get('device_type')

            if device_type == str(integration_type):
                return True

        return False


class NestIntegrationEvent:
    def __init__(
        self,
        event_id: str,
        event_type: Union[IntegrationEventType, str],
        sensor_id: str,
        result: str,
        timestamp: int
    ):
        self.event_type = parse(
            value=event_type,
            enum_type=IntegrationEventType)

        self.result = parse(
            value=result,
            enum_type=IntegrationEventType)

        self.event_id = event_id
        self.sensor_id = sensor_id
        self.timestamp = timestamp

    @staticmethod
    def from_entity(data):
        return NestIntegrationEvent(
            event_id=data.get('event_id'),
            event_type=data.get('event_type'),
            sensor_id=data.get('sensor_id'),
            result=data.get('result'),
            timestamp=data.get('timestamp'))
