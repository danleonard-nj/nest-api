import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
from framework.clients.cache_client import CacheClientAsync
from framework.clients.feature_client import FeatureClientAsync
from framework.concurrency import TaskCollection
from framework.configuration import Configuration
from framework.logger import get_logger
from framework.serialization import Serializable

from clients.nest_client import NestClient
from data.nest_history_repository import NestThermostatHistoryRepository
from data.nest_repository import NestSensorRepository
from domain.enums import Feature, HealthStatus, IntegrationEventType, ThermostatMode
from domain.nest import (NestSensorData, NestSensorDevice, NestThermostat,
                         SensorHealth, SensorHealthStats, SensorPollResult)
from domain.rest import NestSensorDataRequest, SensorDataPurgeResponse
from services.alert_service import AlertService
from services.device_service import NestDeviceService
from services.integration_service import NestIntegrationService
from utils.utils import DateTimeUtil

logger = get_logger(__name__)

SENSOR_UNHEALTHY_SECONDS = 90
DEFAULT_PURGE_DAYS = 180
ALERT_EMAIL_SUBJECT = 'Sensor Failure'
PURGE_EMAIL_SUBJECT = 'Sensor Data Purge'
ALERT_EMAIL_RECIPIENT = 'dcl525@gmail.com'


class NestSensorReduced(Serializable):
    def __init__(
        self,
        device_id: str,
        degrees_fahrenheit: float,
        humidity_percent: float,
        timestamp: int
    ):
        self.device_id = device_id
        self.degrees_fahrenheit = degrees_fahrenheit
        self.humidity_percent = humidity_percent
        self.timestamp = timestamp

    @staticmethod
    def from_sensor(
        sensor
    ):
        return NestSensorReduced(
            device_id=sensor.sensor_id,
            degrees_fahrenheit=sensor.degrees_fahrenheit,
            humidity_percent=sensor.humidity_percent,
            timestamp=sensor.timestamp)


class ThermostatHistory(Serializable):
    def __init__(
        self,
        record_id: str,
        thermostat_id: str,
        mode: str,
        hvac_status,
        target_temperature: float,
        ambient_temperature: float,
        ambient_humidity: float,
        timestamp: int
    ):
        self.record_id = record_id
        self.thermostat_id = thermostat_id
        self.mode = mode
        self.hvac_status = hvac_status
        self.target_temperature = target_temperature
        self.ambient_temperature = ambient_temperature
        self.ambient_humidity = ambient_humidity
        self.timestamp = timestamp

    @staticmethod
    def from_thermostat(
        thermostat: NestThermostat
    ):
        target_temp = 0
        if thermostat.thermostat_mode == ThermostatMode.Cool:
            target_temp = thermostat.cool_fahrenheit
        elif thermostat.thermostat_mode == ThermostatMode.Heat:
            target_temp = thermostat.heat_fahrenheit
        elif thermostat.thermostat_mode == ThermostatMode.HeatCool:
            target_temp = (thermostat.heat_fahrenheit,
                           thermostat.cool_fahrenheit)
        elif thermostat.thermostat_mode == ThermostatMode.Off:
            if thermostat.cool_fahrenheit > 0:
                target_temp = thermostat.cool_fahrenheit
            elif thermostat.heat_fahrenheit > 0:
                target_temp = thermostat.heat_fahrenheit

        return ThermostatHistory(
            record_id=str(uuid.uuid4()),
            thermostat_id=thermostat.thermostat_id,
            mode=thermostat.thermostat_mode,
            hvac_status=thermostat.hvac_status,
            target_temperature=target_temp,
            ambient_temperature=thermostat.ambient_temperature_fahrenheit,
            ambient_humidity=thermostat.humidity_percent,
            timestamp=DateTimeUtil.timestamp())

    @staticmethod
    def from_entity(
        data: Dict
    ):
        return ThermostatHistory(
            record_id=data.get('record_id'),
            thermostat_id=data.get('thermostat_id'),
            mode=data.get('mode'),
            hvac_status=data.get('hvac_status'),
            target_temperature=data.get('target_temperature'),
            ambient_temperature=data.get('ambient_temperature'),
            ambient_humidity=data.get('ambient_humidity'),
            timestamp=data.get('timestamp'))


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
        cache_client: CacheClientAsync,
        feature_client: FeatureClientAsync
    ):
        self.__thermostat_id = configuration.nest.get(
            'thermostat_id')
        self.__purge_days = configuration.nest.get(
            'purge_days', DEFAULT_PURGE_DAYS)

        self.__nest_client = nest_client
        self.__sensor_repository = sensor_repository
        self.__device_service = device_service
        self.__cache_client = cache_client
        self.__integation_service = integration_service
        self.__feature_client = feature_client
        self.__alert_service = alert_service
        self.__thermostat_repository = thermostat_repository

    async def handle_thermostat_history(
        self,
        thermostat: NestThermostat
    ):
        logger.info('Capturing thermostat history')

        history = ThermostatHistory.from_thermostat(
            thermostat=thermostat)

        logger.info(f'History: {history.to_dict()}')

        await self.__thermostat_repository.insert(
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

        data = await self.__nest_client.get_thermostat()
        logger.info(f'Nest thermostat data: {data}')

        thermostat = NestThermostat.from_json_object(
            data=data,
            thermostat_id=self.__thermostat_id)

        return thermostat

    async def log_sensor_data(
        self,
        sensor_request: NestSensorDataRequest
    ) -> NestSensorData:

        sensor = await self.__device_service.get_device(
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
        result = await self.__sensor_repository.insert(
            document=sensor_data.to_dict())

        logger.info(
            f'Capture sensor data for sensor: {sensor.device_name}: {result.acknowledged}')

        return sensor_data

    async def purge_sensor_data(
        self
    ):
        logger.info(f'Purging sensor data: {self.__purge_days} days back')

        # Get the cutoff date and purge any records
        # that step over that line
        cutoff_date = datetime.utcnow() - timedelta(
            days=self.__purge_days)

        cutoff_timestamp = int(cutoff_date.timestamp())
        logger.info(f'Cutoff timestamp: {cutoff_timestamp}')

        result = await self.__sensor_repository.purge_records_before_cutoff(
            cutoff_timestamp=cutoff_timestamp)

        logger.info(f'Deleted: {result.deleted_count}')

        alert_body = self.__get_email_message_body(
            cutoff_date=cutoff_date,
            deleted_count=result.deleted_count)

        # Send an alert email to notify the purge ran
        await self.__alert_service.send_alert(
            recipient=ALERT_EMAIL_RECIPIENT,
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

        hours_back = int(hours_back)
        start_timestamp = now - (hours_back * 60 * 60)

        logger.info(f'Get sensor data: {start_timestamp}: {device_ids}')

        devices = await self.__device_service.get_devices()

        if not any(device_ids):
            logger.info(f'No device IDs provided, using all devices')
            device_ids = [device.device_id for device in devices]

        entities = await self.__sensor_repository.get_sensor_data_by_devices(
            device_ids=device_ids,
            start_timestamp=start_timestamp)

        data = [NestSensorData.from_entity(data=entity)
                for entity in entities]

        reduced = [NestSensorReduced.from_sensor(
            item).to_dict() for item in data]

        device_data = pd.DataFrame([{
            'device_id': device.device_id,
            'device_name': device.device_name
        } for device in devices])

        df = pd.DataFrame(reduced)
        df = df.merge(device_data, on='device_id')

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df = df.set_index('timestamp')

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

        entities = await self.__sensor_repository.get_by_device(
            device_id=sensor_id,
            start_timestamp=start_timestamp)

        data = [NestSensorData.from_entity(data=entity)
                for entity in entities]

        return data

    async def __get_top_sensor_record(
        self,
        device_id: str
    ) -> NestSensorData:

        logger.info(f'Getting top sensor record: {device_id}')
        last_entity = await self.__sensor_repository.get_top_sensor_record(
            sensor_id=device_id)

        if last_entity is None:
            logger.info(f'No sensor data found for device: {device_id}')
            return None

        last_record = NestSensorData.from_entity(
            data=last_entity)

        logger.info(f'Last sensor record: {last_record.to_dict()}')

        return last_record

    def __get_health_status(
        self,
        record: NestSensorData
    ) -> Tuple[str, int]:

        now = DateTimeUtil.timestamp()

        seconds_elapsed = now - record.timestamp

        # Health status (healthy/unhealthy)
        health_status = (
            HealthStatus.Unhealthy
            if seconds_elapsed >= SENSOR_UNHEALTHY_SECONDS
            else HealthStatus.Healthy
        )

        logger.info(f'Seconds elapsed: {seconds_elapsed}: {health_status}')

        return (
            health_status,
            seconds_elapsed
        )

    async def get_sensor_info(
        self
    ) -> List[SensorHealth]:

        logger.info(f'Getting sensor info')
        devices = await self.__device_service.get_devices()

        device_health = list()

        # Handle the sensor health check for a single device
        async def handle_sensor_health_check(
            device: NestSensorDevice
        ) -> None:
            logger.info(f'Calculating health stats: {device.device_id}')

            last_record = await self.__get_top_sensor_record(
                device_id=device.device_id)

            logger.info('Fetched last record successfully')

            health_status, seconds_elapsed = self.__get_health_status(
                record=last_record)

            logger.info(f'Health status: {health_status}: {seconds_elapsed}s')

            stats = SensorHealthStats(
                status=health_status,
                last_contact=last_record.timestamp,
                seconds_elapsed=seconds_elapsed)

            health = SensorHealth(
                device_id=device.device_id,
                device_name=device.device_name,
                health=stats,
                data=last_record)

            logger.info(f'Health: {health.to_dict()}')
            device_health.append(health)

        # Fetch the sensor health info in parallel
        tasks = TaskCollection(*[
            handle_sensor_health_check(device)
            for device in devices
        ])

        await tasks.run()

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
            is_unhealthy = (
                sensor_health.health.status != HealthStatus.Healthy
            )

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
            if self.__integation_service.is_device_integration_supported(
                    device_id=sensor_health.device_id):

                # Get device from cache/db
                device = await self.__device_service.get_device(
                    device_id=sensor_health.device_id)

                logger.info(
                    f'Attempting to power cycle device: {device.device_id}')

                # Handle the sensor integration event
                event_result = await self.__integation_service.handle_integration_event(
                    device=device,
                    event_type=IntegrationEventType.PowerCycle)

                # Add the integration event result info to the response
                device_poll_result.integration = event_result.to_dict()

            logger.info(
                f'Sending unhealthy alert for device: {sensor_health.device_id}')

            is_alert_enabled = await self.__feature_client.is_enabled(
                feature_key=Feature.NestHealthCheckEmailAlerts)
            logger.info(f'Is sensor alert enabled: {is_alert_enabled}')

            # Only send the sensor health alerts if the feature is enabled
            if is_alert_enabled:

                # Get the email body content
                body = self.__get_sensor_failure_email_message_body(
                    device=sensor_health,
                    elapsed_seconds=sensor_health.health.seconds_elapsed)

                await self.__alert_service.send_alert(
                    recipient=ALERT_EMAIL_RECIPIENT,
                    subject=f'{ALERT_EMAIL_SUBJECT}: {sensor_health.device_name}',
                    body=body)

            # Capture the poll result for the sensor
            results.append(device_poll_result)

        logger.info(f'Sorting records by device ID')
        results.sort(key=lambda x: x.device_id)

        return results

    def __get_sensor_failure_email_message_body(
        self,
        device: NestSensorDevice,
        elapsed_seconds: int
    ):
        msg = f"Sensor '{device.device_name}' is unhealthy "
        msg += f"({elapsed_seconds} seconds since last contact)"

        return msg

    def __get_email_message_body(
        self,
        cutoff_date: datetime,
        deleted_count
    ) -> str:
        msg = 'Sensor Data Service\n'
        msg += '\n'
        msg += f'Cutoff Date: {cutoff_date.isoformat()}\n'
        msg += f'Deleted Count: {deleted_count}\n'

        return msg
