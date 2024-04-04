import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
from clients.nest_client import NestClient
from data.nest_history_repository import NestThermostatHistoryRepository
from data.nest_sensor_repository import NestSensorRepository
from domain.enums import Feature, HealthStatus, IntegrationEventType
from domain.nest import (ALERT_EMAIL_SUBJECT, DEFAULT_PURGE_DAYS,
                         DEFAULT_SENSOR_UNHEALTHY_SECONDS, PURGE_EMAIL_SUBJECT,
                         NestSensorData, NestSensorDevice, NestSensorReduced,
                         NestThermostat, SensorHealthStats,
                         SensorHealthSummary, SensorPollResult,
                         ThermostatHistory)
from domain.rest import NestSensorDataRequest, SensorDataPurgeResponse
from framework.clients.feature_client import FeatureClientAsync
from framework.concurrency import TaskCollection
from framework.configuration import Configuration
from framework.logger import get_logger
from services.alert_service import AlertService
from services.device_service import NestDeviceService
from services.integration_service import NestIntegrationService
from utils.utils import DateTimeUtil
from datetime import UTC

logger = get_logger(__name__)


class NestService:
    def __init__(
        self,
        configuration: Configuration,
        nest_client: NestClient,
        sensor_repository: NestSensorRepository,
        device_service: NestDeviceService,
        integration_service: NestIntegrationService,
        thermostat_repository: NestThermostatHistoryRepository,
        alert_service: AlertService,
        feature_client: FeatureClientAsync
    ):
        self._thermostat_id = configuration.nest.get(
            'thermostat_id')
        self._purge_days = configuration.nest.get(
            'purge_days', DEFAULT_PURGE_DAYS)

        self._sensor_unhealthy_seconds = configuration.nest.get(
            'sensor_unhealthy_seconds', DEFAULT_SENSOR_UNHEALTHY_SECONDS)
        self._alert_recipient = configuration.nest.get(
            'alert_recipient')

        self._nest_client = nest_client
        self._sensor_repository = sensor_repository
        self._device_service = device_service
        self._integation_service = integration_service
        self._feature_client = feature_client
        self._alert_service = alert_service
        self._thermostat_repository = thermostat_repository

    async def handle_thermostat_history(
        self,
        thermostat: NestThermostat
    ):
        logger.info('Capturing thermostat history')

        history = ThermostatHistory.from_thermostat(
            thermostat=thermostat)

        logger.info(f'History: {history.to_dict()}')

        await self._thermostat_repository.insert(
            document=history.to_dict())

        return history

    async def capture_thermostat_history(
        self
    ):
        logger.info('Capturing thermostat history')

        # Fetch the current thermostat state
        thermostat = await self.get_thermostat()

        if thermostat is None:
            logger.info('No thermostat found')
            raise Exception('No thermostat found')

        # Store the thermostat history
        history = await self.handle_thermostat_history(
            thermostat=thermostat)

        return history

    async def get_thermostat(
        self
    ) -> NestThermostat:

        data = await self._nest_client.get_thermostat()
        logger.info(f'Nest thermostat data: {data}')

        thermostat = NestThermostat.from_response(
            data=data,
            thermostat_id=self._thermostat_id)

        return thermostat

    async def log_sensor_data(
        self,
        sensor_request: NestSensorDataRequest
    ) -> NestSensorData:

        sensor = await self._device_service.get_device(
            device_id=sensor_request.sensor_id)

        if sensor is None:
            logger.info(f'Sensor not found: {sensor_request.sensor_id}')
            raise Exception(
                f"No sensor with the ID '{sensor_request.sensor_id}' exists")

        # Create the sensor data record w/ stats
        sensor_data = NestSensorData(
            record_id=str(uuid.uuid4()),
            sensor_id=sensor_request.sensor_id,
            humidity_percent=sensor_request.humidity_percent,
            degrees_celsius=sensor_request.degrees_celsius,
            diagnostics=sensor_request.diagnostics,
            timestamp=DateTimeUtil.timestamp())

        # Write the new sensor data
        result = await self._sensor_repository.insert(
            document=sensor_data.to_dict())

        logger.info(
            f'Capture sensor data for sensor: {sensor.device_name}: {result.acknowledged}')

        return sensor_data

    async def purge_sensor_data(
        self
    ):
        logger.info(f'Purging sensor data: {self._purge_days} days back')

        # Get the cutoff date and purge any records
        # that step over that line
        cutoff_date = datetime.now(UTC) - timedelta(
            days=self._purge_days)

        cutoff_timestamp = int(cutoff_date.timestamp())
        logger.info(f'Cutoff timestamp: {cutoff_timestamp}')

        result = await self._sensor_repository.purge_records_before_cutoff(
            cutoff_timestamp=cutoff_timestamp)

        logger.info(f'Deleted: {result.deleted_count}')

        alert_body = self._get_email_message_body(
            cutoff_date=cutoff_date,
            deleted_count=result.deleted_count)

        # Send an alert email to notify the purge ran
        await self._alert_service.send_alert(
            recipient=self._alert_recipient,
            subject=PURGE_EMAIL_SUBJECT,
            body=alert_body)

        return SensorDataPurgeResponse(
            deleted=result.deleted_count)

    async def get_sensor_data(
        self,
        hours_back: int,
        device_ids: List[str],
        sample: str
    ) -> List[Dict[str, List[NestSensorData]]]:

        now = DateTimeUtil.timestamp()
        start_timestamp = now - (int(hours_back) * 60 * 60)

        logger.info(f'Get sensor data: {start_timestamp}: {device_ids}')

        devices = await self._device_service.get_devices()

        if not any(device_ids):
            device_ids = [device.device_id for device in devices]

        logger.info(f'Fetching data for sensors: {device_ids}')
        entities = await self._sensor_repository.get_sensor_data_by_devices(
            device_ids=device_ids,
            start_timestamp=start_timestamp)

        logger.info(f'Fetched {len(entities)} records')

        # TODO: Simplify this and remove the reduced model
        data = [NestSensorData.from_entity(data=entity)
                for entity in entities]

        reduced = [NestSensorReduced.from_sensor(
            item).to_dict() for item in data]

        # Get a lookup df of the device IDs and names
        device_data = pd.DataFrame([{
            'device_id': device.device_id,
            'device_name': device.device_name
        } for device in devices])

        df = pd.DataFrame(reduced)

        # Merge the device lookup df on the sensor data
        df = df.merge(device_data, on='device_id')

        # Handle datetimes and set the index
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df = df.set_index('timestamp')

        # Group by and sample data
        df = df.groupby(['device_id', 'device_name']).resample(sample).mean()
        df = df.reset_index()

        return df.to_dict(orient='records')

    async def get_sensor_history(
        self,
        sensor_id: str,
        hours_back: int
    ):
        now = DateTimeUtil.timestamp()

        hours_back = int(hours_back)
        start_timestamp = now - (hours_back * 60 * 60)

        entities = await self._sensor_repository.get_by_device(
            device_id=sensor_id,
            start_timestamp=start_timestamp)

        data = [NestSensorData.from_entity(data=entity)
                for entity in entities]

        return data

    async def _get_top_sensor_record(
        self,
        device_id: str
    ) -> NestSensorData | None:

        last_entity = await self._sensor_repository.get_top_sensor_record(
            sensor_id=device_id)

        if last_entity is None:
            logger.info(f'No sensor data found for device: {device_id}')
            return None

        last_record = NestSensorData.from_entity(
            data=last_entity)

        return last_record

    def _get_health_status(
        self,
        record: NestSensorData
    ) -> Tuple[str, int]:

        now = DateTimeUtil.timestamp()

        seconds_elapsed = now - record.timestamp

        # Determine health status
        health_status = (
            HealthStatus.Unhealthy
            if seconds_elapsed >= self._sensor_unhealthy_seconds
            else HealthStatus.Healthy
        )

        logger.info(f'Seconds elapsed: {seconds_elapsed}: {health_status}')

        return (
            health_status,
            seconds_elapsed
        )

    async def _handle_sensor_health_check(
        device: NestSensorDevice
    ) -> None:

        last_record = await self._get_top_sensor_record(
            device_id=device.device_id)

        health_status, seconds_elapsed = self._get_health_status(
            record=last_record)

        logger.info(f'Health status: {device.device_id}: {health_status}: {seconds_elapsed}s')

        stats = SensorHealthStats(
            status=health_status,
            last_contact=last_record.timestamp,
            seconds_elapsed=seconds_elapsed)

        health = SensorHealthSummary(
            device_id=device.device_id,
            device_name=device.device_name,
            health=stats,
            data=last_record)

        return health

    async def get_sensor_info(
        self
    ) -> List[SensorHealthSummary]:

        logger.info(f'Getting sensor info')
        devices = await self._device_service.get_devices()

        # Fetch the sensor health info in parallel
        device_health = await TaskCollection(*[
            self._handle_sensor_health_check(device)
            for device in devices
        ]).run()

        logger.info(f'Sorting results by device name')
        device_health.sort(key=lambda x: x.device_name)

        return device_health

    async def poll_sensor_status(
        self
    ):
        logger.info(f'Polling sensor status')

        # Get the sensor health info
        sensors = await self.get_sensor_info()
        results = list()

        for sensor_health in sensors:

            logger.info(f'Checking sensor health: {sensor_health.device_id}')

            # Sensor health
            is_unhealthy = sensor_health.health.status != HealthStatus.Healthy
            logger.info(f'Is unhealthy: {is_unhealthy}')

            # If the sensor is healthy then skip it
            if not is_unhealthy:
                logger.info(f'Sensor is healthy: {sensor_health.device_id}')
                continue

            device_poll_result = SensorPollResult(
                device_id=sensor_health.device_id,
                is_healthy=not is_unhealthy)

            logger.info('Checking for sensor power cycle integration')

            # Check for sensor integrations like power cycling or fans
            if self._integation_service.is_device_integration_supported(
                    device_id=sensor_health.device_id):

                # Get device from cache/db
                device = await self._device_service.get_device(
                    device_id=sensor_health.device_id)

                logger.info(
                    f'Attempting to power cycle device: {device.device_id}')

                # Handle the sensor integration event
                event_result = await self._integation_service.handle_integration_event(
                    device=device,
                    event_type=IntegrationEventType.PowerCycle)

                # Add the integration event result info to the response
                device_poll_result.integration = event_result.to_dict()

            logger.info(
                f'Sending unhealthy alert for device: {sensor_health.device_id}')

            is_alert_enabled = await self._feature_client.is_enabled(
                feature_key=Feature.NestHealthCheckEmailAlerts)
            logger.info(f'Is sensor alert enabled: {is_alert_enabled}')

            # Only send the sensor health alerts if the feature is enabled
            if is_alert_enabled:

                # Get the email body content
                body = self._get_sensor_failure_email_message_body(
                    device=sensor_health,
                    elapsed_seconds=sensor_health.health.seconds_elapsed)

                await self._alert_service.send_alert(
                    recipient=self._alert_recipient,
                    subject=f'{ALERT_EMAIL_SUBJECT}: {sensor_health.device_name}',
                    body=body)

            # Capture the poll result for the sensor
            results.append(device_poll_result)

        logger.info(f'Sorting records by device ID')
        results.sort(key=lambda x: x.device_id)

        return results

    def _get_sensor_failure_email_message_body(
        self,
        device: NestSensorDevice,
        elapsed_seconds: int
    ):
        msg = f"Sensor '{device.device_name}' is unhealthy "
        msg += f"({elapsed_seconds} seconds since last contact)"

        return msg

    def _get_email_message_body(
        self,
        cutoff_date: datetime,
        deleted_count: int
    ) -> str:
        msg = 'Sensor Data Service\n'
        msg += '\n'
        msg += f'Cutoff Date: {cutoff_date.isoformat()}\n'
        msg += f'Deleted Count: {deleted_count}\n'

        return msg
