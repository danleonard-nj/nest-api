from typing import Dict, List, Union

from framework.serialization import Serializable

from domain.enums import (IntegrationEventResult, IntegrationEventType,
                          IntergationDeviceType, KasaIntegrationSceneType)
from utils.helpers import parse


class DeviceIntegrationSceneMapping(Serializable):
    def __init__(
        self,
        device_type: Union[IntergationDeviceType, str],
        scenes: Dict
    ):
        self.scenes = scenes
        self.device_type = parse(
            value=device_type,
            enum_type=IntergationDeviceType)

    @staticmethod
    def from_config(
        data: Dict
    ):
        return DeviceIntegrationSceneMapping(
            device_type=data.get('device_type'),
            scenes=data.get('scenes'))

    def get_scene(
        self,
        scene_type: Union[KasaIntegrationSceneType, str]
    ) -> Union[str, None]:

        scene_type = parse(
            value=scene_type,
            enum_type=KasaIntegrationSceneType)

        return self.scenes.get(scene_type)


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
            if integration.get('device_type') == str(integration_type):
                return True
        return False

    def get_integration_data(
        self,
        integration_device_type: Union[IntergationDeviceType, str]
    ) -> Union[Dict, None]:

        for integration in self.integrations:
            if integration.get('device_type') == str(integration_device_type):
                return integration
        return None


class NestIntegrationEvent(Serializable):
    def __init__(
        self,
        event_id: str,
        event_type: Union[IntegrationEventType, str],
        sensor_id: str,
        result: Union[IntegrationEventResult, str],
        timestamp: int
    ):
        self.event_id = event_id
        self.sensor_id = sensor_id

        self.event_type = parse(
            value=event_type,
            enum_type=IntegrationEventType)

        self.result = parse(
            value=result,
            enum_type=IntegrationEventResult)

        self.timestamp = timestamp

    @staticmethod
    def from_entity(data):
        return NestIntegrationEvent(
            event_id=data.get('event_id'),
            event_type=data.get('event_type'),
            sensor_id=data.get('sensor_id'),
            result=data.get('result'),
            timestamp=data.get('timestamp'))
