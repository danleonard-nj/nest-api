import asyncio
import uuid

import pandas as pd
from clients.kasa_client import KasaClient
from data.nest_integration_repository import NestIntegrationRepository
from domain.enums import (IntegrationEventResult, IntegrationEventType,
                          IntergationDeviceType, KasaIntegrationSceneType)
from domain.integration import (DeviceIntegrationConfig,
                                DeviceIntegrationSceneMapping,
                                NestIntegrationEvent)
from domain.nest import EMAIL_ALERT_FEATURE_KEY, NestSensorDevice
from domain.rest import (HandleIntegrationEventResponse,
                         IntegrationEventResponse)
from framework.clients.feature_client import FeatureClientAsync
from framework.configuration import Configuration
from framework.logger import get_logger
from framework.validators.nulls import none_or_whitespace
from services.alert_service import AlertService
from services.device_service import NestDeviceService
from framework.concurrency import TaskCollection
from utils.helpers import parse
from utils.utils import DateTimeUtil

logger = get_logger(__name__)


class NestIntegrationService:
    @property
    def integrations(
        self
    ):
        if self._integrations is None:
            self._integrations = self._load_integration_lookup(
                data=self._configuration.kasa)
        return self._integrations

    def __init__(
        self,
        configuration: Configuration,
        integration_repository: NestIntegrationRepository,
        device_service: NestDeviceService,
        kasa_client: KasaClient,
        alert_service: AlertService,
        feature_client: FeatureClientAsync
    ):
        self._configuration = configuration
        self._integration_repository = integration_repository
        self._device_service = device_service
        self._kasa_client = kasa_client
        self._alert_service = alert_service
        self._feature_client = feature_client

        self._minimum_integration_interval = configuration.nest.get(
            'minimum_integration_interval')
        self._alert_recipient = configuration.nest.get(
            'alert_recipient')
        self._integration_power_cycle_seconds = configuration.nest.get(
            'integration_power_cycle_seconds')

        self._integrations = None

    def is_device_integration_supported(
        self,
        device_id: str
    ):
        return device_id in self.integrations

    async def get_integration_events(
        self,
        days_back: int,
        sensor_id: str = None
    ) -> list[IntegrationEventResponse]:

        end_timestamp = DateTimeUtil.timestamp()
        start_timestamp = end_timestamp - (int(days_back) * 24 * 60 * 60)

        logger.info(f'Fetching integration events: {start_timestamp} -> {end_timestamp}')

        devices, entities = await TaskCollection(
            self._device_service.get_devices(),
            self._integration_repository.get_integration_events(
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                sensor_id=sensor_id)).run()

        logger.info(f'Integration events fetched: {len(entities)}')

        events = [NestIntegrationEvent.from_entity(data=entity)
                  for entity in entities]

        if not any(events):
            logger.info(f'No events found in range: {start_timestamp} to {end_timestamp}')
            return list()

        df = self._merge_devices_on_events(
            devices=devices,
            events=events)

        df = df.sort_values(
            by='timestamp',
            ascending=False)

        return [IntegrationEventResponse.from_dict(data=result)
                for result in df.to_dict(orient='records')]

    def _merge_devices_on_events(
        self,
        devices: list[NestSensorDevice],
        events: list[NestIntegrationEvent]
    ) -> pd.DataFrame:

        devices_df = pd.DataFrame([x.to_dict() for x in devices])
        devices_df = devices_df[['device_id', 'device_name']]

        events_df = pd.DataFrame([x.to_dict() for x in events])

        return events_df.merge(
            right=devices_df,
            left_on='sensor_id',
            right_on='device_id',
            how='left')

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
        latest_event_entity = await self._integration_repository.get_latest_integation_event_by_sensor(
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
                    (self._minimum_integration_interval * 60)):

                logger.info(
                    f"Minimum interval of '{self._minimum_integration_interval}' minutes has not passed since the last event")

                return HandleIntegrationEventResponse(
                    integration_event_type=event_type,
                    result=IntegrationEventResult.MinimumInterval,
                    message='The minimum interval has not passed since the last event')

        # Handle power cycle integration events
        if event_type == IntegrationEventType.PowerCycle:
            power_cycle_result = await self._handle_power_cycle_integration_event(
                sensor=device,
                integration_config=config,
                integration_event_type=event_type)

            # Only send an alert message for certain result types
            if power_cycle_result.result in [IntegrationEventResult.Success,
                                             IntegrationEventResult.Failure,
                                             IntegrationEventResult.Error]:

                logger.info(
                    f'Sending alert for power cycle result: {power_cycle_result.result}')

                is_enabled = await self._feature_client.is_enabled(
                    feature_key=EMAIL_ALERT_FEATURE_KEY)

                if is_enabled:
                    await self._send_intergration_event_alert(
                        sensor=device,
                        event_type=event_type,
                        data=power_cycle_result.to_dict())

            return power_cycle_result

        else:
            return HandleIntegrationEventResponse(
                integration_event_type=event_type,
                result=IntegrationEventResult.NoAction,
                message=f'No action was taken for the event type: {event_type}')

    async def _handle_power_cycle_integration_event(
        self,
        sensor: NestSensorDevice,
        integration_config: DeviceIntegrationConfig,
        integration_event_type: IntegrationEventType | str
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
        mapping = DeviceIntegrationSceneMapping.from_config(
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
            power_off_status, _ = await self._kasa_client.run_scene(
                scene_id=power_off)
            logger.info(f'Power off response: {power_off_status}')

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

        logger.info(f'Sleeping {self._integration_power_cycle_seconds} seconds')

        await asyncio.sleep(self._integration_power_cycle_seconds)

        try:
            # Send the request to run the power on scene
            power_on_status, _ = await self._kasa_client.run_scene(
                scene_id=power_on)
            logger.info(f'Power on response: {power_on_status}')

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

        insert_result = await self._integration_repository.insert(
            document=integration_event.to_dict())

        logger.info(
            f'Integration event insert result: {insert_result.inserted_id}')

        return HandleIntegrationEventResponse(
            integration_event_type=integration_event_type,
            result=IntegrationEventResult.Success,
            message='The integration event was handled successfully')

    async def _send_intergration_event_alert(
        self,
        sensor: NestSensorDevice,
        event_type: IntegrationEventType,
        data: dict
    ):
        subject = f'Integration Event For Sensor {sensor.device_name}: {event_type}'

        if not isinstance(data, list):
            data = [data]

        for row in data:
            row['timestamp'] = DateTimeUtil.az_local()

        await self._alert_service.send_datatable_email(
            recipient=self._alert_recipient,
            subject=subject,
            data=data)

    def _load_integration_lookup(
        self,
        data: dict
    ) -> dict[str, DeviceIntegrationConfig]:

        devices = data.get('devices', list())

        logger.info(f'Loading device integrations: {devices}')

        integrations = [DeviceIntegrationConfig.from_json_object(data=device)
                        for device in devices]

        logger.info(f'{len(integrations)} devices intergration configs loaded')

        return {
            di.sensor_id: di for di in integrations
        }
