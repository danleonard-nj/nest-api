import asyncio
import uuid
from typing import Dict, Union

from framework.configuration import Configuration
from framework.logger import get_logger
from framework.serialization import Serializable
from framework.validators.nulls import none_or_whitespace

from clients.email_gateway_client import EmailGatewayClient
from clients.kasa_client import KasaClient
from data.nest_integration_repository import NestIntegrationRepository
from domain.enums import (IntegrationEventResult, IntegrationEventType,
                          IntergationDeviceType, KasaIntegrationSceneType)
from domain.integration import (DeviceIntegrationConfig,
                                DeviceIntegrationSceneMappingConfig,
                                NestIntegrationEvent)
from domain.nest import NestSensorDevice
from services.event_service import EventService
from utils.helpers import parse
from utils.utils import DateTimeUtil

logger = get_logger(__name__)

MINIMUM_EVENT_INTERVAL_MINUTES = 60
ALERT_RECIPIENT = 'dcl525@gmail.com'
POWER_CYCLE_INTERVAL_SECONDS = 5


class HandleIntegrationEventResponse(Serializable):
    def __init__(
        self,
        result: Union[str, IntegrationEventResult],
        message: str = None,
        integration_event_type: Union[str, IntegrationEventType] = None
    ):
        self.event_type = integration_event_type
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

    def is_device_integration_supported(
        self,
        device_id: str
    ):
        return device_id in self.integrations

    async def get_integration_events(
        self,
        start_timestamp: int,
        end_timestamp: int = None,
        max_results: int = None
    ):
        end_timestamp = (
            end_timestamp or DateTimeUtil.timestamp()
        )

        start_timestamp = int(start_timestamp)
        end_timestamp = int(end_timestamp)

        logger.info(
            f'Getting integration events: {start_timestamp} to {end_timestamp}')

        entities = await self.__integration_repository.get_integration_events(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            max_results=max_results)

        logger.info(f'Integration events fetched: {len(entities)}')

        events = [NestIntegrationEvent.from_entity(data=entity)
                  for entity in entities]

        return events

    async def handle_integration_event(
        self,
        device: NestSensorDevice,
        event_type: IntegrationEventType
    ):
        sensor_id = device.device_id

        config = self.integrations.get(sensor_id)

        # No integration config is defined for the sensor
        if config is None:
            raise Exception(
                f"No integration config is defined for sensor with the ID '{sensor_id}'")

        # Get the latest integration event for the sensor
        latest_event_entity = await self.__integration_repository.get_latest_integation_event_by_sensor(
            sensor_id=sensor_id)

        # If we have a stored integration event for the sensor
        # verify the minimum interval has passed since the last
        # event occured
        if latest_event_entity is not None:
            logger.info(f'Stored event exists: {latest_event_entity}')

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
                    integration_event_type=event_type,
                    result=IntegrationEventResult.MinimumIntervalNotMet,
                    message='The minimum interval has not passed since the last event')

        # Handle power cycle integration events
        if event_type == IntegrationEventType.PowerCycle:
            power_cycle_result = await self.__handle_power_cycle_integration_event(
                sensor=device,
                integration_config=config,
                integration_event_type=event_type)

            # Only send an alert message for certain result types
            if power_cycle_result.result in [IntegrationEventResult.Success,
                                             IntegrationEventResult.Failure,
                                             IntegrationEventResult.Error]:

                await self.__send_intergration_event_alert(
                    sensor=device,
                    event_type=event_type,
                    result=power_cycle_result.result)

            return power_cycle_result

        else:
            return HandleIntegrationEventResponse(
                integration_event_type=event_type,
                result=IntegrationEventResult.NoOp,
                message=f'No action was taken for the event type: {event_type}')

    async def __handle_power_cycle_integration_event(
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
                integration_event_type=integration_event_type,
                result=IntegrationEventResult.NotSupported,
                message='The sensor does not support the integration type')

        integration = integration_config.get_integration_data(
            integration_device_type=IntergationDeviceType.Plug)

        # Build the config object for the device integration
        mapping = DeviceIntegrationSceneMappingConfig.from_json_object(
            data=integration)

        logger.info(f'Integration mapping: {mapping.to_dict()}')

        # Get the scene ID for the power off phase
        logger.info(f'Getting scene for power off phase')
        power_off = mapping.get_scene(
            scene_type=KasaIntegrationSceneType.PowerOff)

        # Verify a scene is defined for the power off phase
        if none_or_whitespace(power_off):
            logger.info(f'No scene ID was found for power off phase')

            return HandleIntegrationEventResponse(
                integration_event_type=integration_event_type,
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
                integration_event_type=integration_event_type,
                result=IntegrationEventResult.InvalidConfiguration,
                message='No scene ID was found for power on phase')

        logger.info(f'Sending request to run scene for power off')

        try:
            # Send the request to run the power off scene
            power_off_status, client_response_power_off = await self.__kasa_client.run_scene(
                scene_id=power_off)
            logger.info(f'Power off response: {client_response_power_off}')

            # Verify power on scene ran successfully
            if power_off_status != 200:
                raise Exception(
                    f'Power off scene failed with status code: {power_off_status}')

        except Exception as ex:
            logger.info(f'Failed to run power off scene: {str(ex)}')

            # Bail out if we fail to run power off
            return HandleIntegrationEventResponse(
                integration_event_type=integration_event_type,
                result=IntegrationEventResult.Error,
                message=f'An error occurred while sending the request to run the power off scene: {str(ex)}')

        logger.info(
            f'Sleeping for cycle interval {POWER_CYCLE_INTERVAL_SECONDS} seconds')
        await asyncio.sleep(POWER_CYCLE_INTERVAL_SECONDS)

        try:
            # Send the request to run the power on scene
            power_on_status, client_response_power_on = await self.__kasa_client.run_scene(
                scene_id=power_on)
            logger.info(f'Power on response: {client_response_power_on}')

            # Verify power off scene ran successfully
            if power_on_status != 200:
                raise Exception(
                    f'Power on scene failed with status code: {power_on_status}')

        except Exception as ex:

            # TODO: Send an alert email as the plug may need to be manually power cycled
            return HandleIntegrationEventResponse(
                integration_event_type=integration_event_type,
                result=IntegrationEventResult.Error,
                message=f'An error occurred while sending the request to run the power on scene: {str(ex)}')

        # Create the integration event entity
        integration_event = NestIntegrationEvent(
            event_id=str(uuid.uuid4()),
            sensor_id=sensor_id,
            event_type=integration_event_type,
            result=IntegrationEventResult.Success,
            timestamp=DateTimeUtil.timestamp())

        insert_result = await self.__integration_repository.insert(
            document=integration_event.to_dict())

        logger.info(
            f'Integration event insert result: {insert_result.inserted_id}')

        return HandleIntegrationEventResponse(
            integration_event_type=integration_event_type,
            result=IntegrationEventResult.Success,
            message='The integration event was handled successfully')

    async def __send_intergration_event_alert(
        self,
        sensor: NestSensorDevice,
        event_type: IntegrationEventType,
        result: IntegrationEventResult
    ):
        subject = f'Integration event alert: {event_type}'

        message = f'An integration event has occurred for the sensor with the ID: {sensor.device_id}\n'
        message += '\n'
        message += f'Event type: {event_type}\n'
        message += f'Event result: {result}\n'

        email_request, endpoint = self.__email_client.get_email_request(
            recipient=ALERT_RECIPIENT,
            subject=subject,
            body=message)

        logger.info(f'Sending email alert: {email_request.to_dict()}')
        logger.info(f'Endpoint: {endpoint}')

        await self.__event_service.dispatch_email_event(
            endpoint=endpoint,
            message=email_request.to_dict())

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
