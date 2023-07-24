from typing import Dict, Union

from framework.configuration import Configuration
from framework.logger import get_logger
from framework.serialization import Serializable

from clients.email_gateway_client import EmailGatewayClient
from clients.kasa_client import KasaClient
from data.nest_integration_repository import NestIntegrationRepository
from domain.enums import IntegrationEventResult, IntegrationEventType, IntergationDeviceType, KasaIntegrationSceneType
from domain.integration import DeviceIntegrationConfig, DeviceIntegrationSceneMappingConfig, NestIntegrationEvent
from domain.nest import NestSensorDevice
from framework.validators.nulls import none_or_whitespace
from services.event_service import EventService
from utils.helpers import parse
from utils.utils import DateTimeUtil

logger = get_logger(__name__)

MINIMUM_EVENT_INTERVAL_MINUTES = 60
ALERT_RECIPIENT = 'dcl525@gmail.com'


class HandleIntegrationEventResponse(Serializable):
    def __init__(
        self,
        result: Union[str, IntegrationEventResult],
        message: str = None
    ):
        self.message = message or str(result)
        self.result = parse(result, IntegrationEventResult)


class NestIntegrationService:
    @property
    def integrations(
        self
    ):
        if self.__integrations is None:
            self.__integrations = self.__load_integration_lookup(
                data=self.__configuration.kasa)
        return self.__integrations

    def __init__(
        self,
        configuration: Configuration,
        integration_repository: NestIntegrationRepository,
        email_client: EmailGatewayClient,
        event_service: EventService,
        kasa_client: KasaClient
    ):
        self.__configuration = configuration
        self.__integration_repository = integration_repository
        self.__email_client = email_client
        self.__event_service = event_service
        self.__kasa_client = kasa_client

        self.__integrations = None

    def __load_integration_lookup(
        self,
        data: Dict
    ) -> Dict[str, DeviceIntegrationConfig]:

        devices = data.get('devices', list())
        logger.info(f'Loading device integrations: {devices}')

        integrations = [DeviceIntegrationConfig.from_json_object(data=device)
                        for device in devices]

        lookup = {
            di.sensor_id: di for di in integrations
        }

        logger.info(f'{len(integrations)} devices intergration configs loaded')

        return lookup

    async def send_intergration_event_alert(
        self,
        sensor: NestSensorDevice,
        event_type: IntegrationEventType,
        result: IntegrationEventResult
    ):
        subject = f'Integration event alert: {event_type}'

        message = f'An integration event has occurred for the sensor with the ID: {sensor.device_id}'
        message += '\n'
        message += f'Event type: {event_type}'
        message += f'Event result: {result}'

        email_request, endpoint = self.__email_client.get_email_request(
            recipient=ALERT_RECIPIENT,
            subject=subject,
            body=message)

        logger.info(f'Sending email alert: {email_request.to_dict()}')
        logger.info(f'Endpoint: {endpoint}')

        await self.__event_service.dispatch_email_event(
            endpoint=endpoint,
            message=email_request.to_dict())

    async def handle_integration_event(
        self,
        sensor: NestSensorDevice,
        event_type: IntegrationEventType
    ):
        sensor_id = sensor.device_id

        config = self.__integrations.get(sensor_id)

        # No integration config is defined for the sensor
        if config is None:
            raise Exception(
                f"No integration config is defined for sensor with the ID '{sensor_id}'")

        # Get the latest integration event for the sensor
        latest_event_entity = await self.__integration_repository.get_latest_integation_event_by_sensor_id(
            sensor_id=sensor_id)

        # If we have a stored integration event for the sensor
        # verify the minimum interval has passed since the last
        # event occured
        if latest_event_entity is not None:
            latest_event = NestIntegrationEvent.from_entity(
                data=latest_event_entity)

            now = DateTimeUtil.timestamp()

            # If the minimum interval hasn't been met since the
            # last integration event
            if (now - latest_event.timestamp <
                    (MINIMUM_EVENT_INTERVAL_MINUTES * 60)):

                logger.info(
                    f"Minimum interval of '{MINIMUM_EVENT_INTERVAL_MINUTES}' minutes has not passed since the last event")

                return HandleIntegrationEventResponse(
                    result=IntegrationEventResult.MinimumIntervalNotMet,
                    message='The minimum interval has not passed since the last event')

        if event_type == IntegrationEventType.PowerCycle:
            return await self.handle_power_cycle_integration_event(
                sensor=sensor,
                integration_config=config,
                integration_event_type=event_type)

        else:
            return HandleIntegrationEventResponse(
                result=IntegrationEventResult.NoOp,
                message=f'No action was taken for the event type: {event_type}')

    async def handle_power_cycle_integration_event(
        self,
        sensor: NestSensorDevice,
        integration_config: DeviceIntegrationConfig,
        integration_event_type: Union[IntegrationEventType, str]
    ):
        sensor_id = sensor.device_id
        integration_event_type = parse(
            integration_event_type, IntegrationEventType)

        # Verify this type of integration event is configured
        # for this sensor
        if not integration_config.is_supported(
                integration_type=IntergationDeviceType.Plug):

            logger.info(
                f"Sensor with the ID '{sensor_id}' does not support the '{IntergationDeviceType.Plug}' integration type")

            return HandleIntegrationEventResponse(
                result=IntegrationEventResult.NotSupported,
                message='The sensor does not support the integration type')

        integration = integration_config.get_integration_data(
            integration_device_type=IntergationDeviceType.Plug)

        mapping = DeviceIntegrationSceneMappingConfig.from_json_object(
            data=integration)

        logger.info(f'Integration mapping: {mapping.to_json_object()}')

        # Get the scene ID for the power off phase
        logger.info(f'Getting scene for power off phase')
        power_off = mapping.get_scene(
            scene_type=KasaIntegrationSceneType.PowerOff)

        # Verify a scene is defined for the power off phase
        if none_or_whitespace(power_off):
            logger.info(f'No scene ID was found for power off phase')

            return HandleIntegrationEventResponse(
                result=IntegrationEventResult.InvalidConfiguration,
                message='No scene ID was found for power off phase')

        logger.info(f'Power off scene ID: {power_off}')

        logger.info(f'Getting scene for power on phase')
        power_on = mapping.get_scene(
            scene_type=KasaIntegrationSceneType.PowerOn)

        # Verify a scene is defined for the power on phase
        if none_or_whitespace(power_on):
            logger.info(f'No scene ID was found for power on phase')

            return HandleIntegrationEventResponse(
                result=IntegrationEventResult.InvalidConfiguration,
                message='No scene ID was found for power on phase')
