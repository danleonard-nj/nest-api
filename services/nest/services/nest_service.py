import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
from framework.clients.cache_client import CacheClientAsync
from framework.concurrency import TaskCollection
from framework.configuration import Configuration
from framework.logger import get_logger

from clients.email_gateway_client import EmailGatewayClient
from clients.nest_client import NestClient
from data.nest_repository import NestDeviceRepository, NestSensorRepository
from domain.cache import CacheKey
from domain.enums import HealthStatus
from domain.nest import (NestSensorData, NestSensorDataQueryResponse,
                         NestSensorDevice, NestThermostat, SensorHealth,
                         SensorHealthStats, SensorPollResult)
from domain.rest import NestSensorDataRequest, SensorDataPurgeResponse
from services.device_service import NestDeviceService
from services.event_service import EventService
from services.integration_service import NestIntegrationService
from utils.utils import DateTimeUtil

logger = get_logger(__name__)

SENSOR_UNHEALTHY_SECONDS = 90
DEFAULT_PURGE_DAYS = 90
ALERT_EMAIL_SUBJECT = 'ESP8266 Sensor Failure'
ALERT_EMAIL_RECIPIENT = 'dcl525@gmail.com'


class NestService:
    def __init__(
        self,
        configuration: Configuration,
        nest_client: NestClient,
        sensor_repository: NestSensorRepository,
        device_service: NestDeviceService,
        # integration_service: NestIntegrationService,
        event_service: EventService,
        email_gateway: EmailGatewayClient,
        cache_client: CacheClientAsync
    ):
        self.__thermostat_id = configuration.nest.get(
            'thermostat_id')
        self.__purge_days = configuration.nest.get(
            'purge_days', DEFAULT_PURGE_DAYS)
        self.__purge_days = configuration.nest.get(
            'purge_days', DEFAULT_PURGE_DAYS)

        self.__nest_client = nest_client
        self.__sensor_repository = sensor_repository
        self.__device_service = device_service
        self.__event_service = event_service
        self.__email_gateway = email_gateway
        self.__cache_client = cache_client

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

        logger.info(f'Logging sensor data: {sensor_request}')
        sensor = await self.__device_service.get_device(
            device_id=sensor_request.sensor_id)

        if sensor is None:
            logger.info(f'Sensor not found: {sensor_request.sensor_id}')
            raise Exception(
                f"No sensor with the ID '{sensor_request.sensor_id}' exists")

        last_record = await self.__get_top_sensor_record(
            device_id=sensor_request.sensor_id)

        # Create the sensor data record w/ stats
        sensor_data = NestSensorData(
            record_id=str(uuid.uuid4()),
            sensor_id=sensor_request.sensor_id,
            humidity_percent=sensor_request.humidity_percent,
            degrees_celsius=sensor_request.degrees_celsius,
            diagnostics=sensor_request.diagnostics,
            timestamp=DateTimeUtil.timestamp())

        logger.info(f'Capturing sensor data: {sensor_data.to_dict()}')

        # Write the new sensor data
        result = await self.__sensor_repository.insert(
            document=sensor_data.to_dict())

        logger.info(f'Success: {result.acknowledged}')

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

        result = await self.__sensor_repository.collection.delete_many({
            'timestamp': {
                '$lte': cutoff_timestamp
            }
        })

        logger.info(f'Deleted: {result.deleted_count}')

        email_request, endpoint = self.__email_gateway.get_email_request(
            recipient='dcl525@gmail.com',
            subject='Sensor Data Service',
            body=self.__get_email_message_body(
                cutoff_date=cutoff_date,
                deleted_count=result.deleted_count
            ))

        logger.info(
            f'Dispatching email event message: {email_request.to_dict()}')

        await self.__event_service.dispatch_email_event(
            endpoint=endpoint,
            message=email_request.to_dict())

        return SensorDataPurgeResponse(
            deleted=result.deleted_count)

    async def get_sensor_data(
        self,
        start_timestamp: int
    ) -> List[Dict[str, List[NestSensorData]]]:

        logger.info(f'Get sensor data: {start_timestamp}')
        devices = await self.__device_service.get_devices()

        results = list()

        for device in devices:
            logger.info(f'Fetching data for device: {device.device_id}')

            entities = await self.__sensor_repository.get_by_device(
                device_id=device.device_id,
                start_timestamp=start_timestamp)

            data = [NestSensorData.from_entity(data=entity)
                    for entity in entities]

            result = {
                'device_id': device.device_id,
                'data': data
            }

            logger.info(
                f'Device: {device.device_id}: {len(data)} records fetched')

            results.append(result)

        return results

    async def get_grouped_sensor_data(
        self,
        start_timestamp: int
    ):
        data = await self.get_sensor_data(
            start_timestamp=start_timestamp)

        tasks = TaskCollection()

        for device in data:
            device_id = device.get('device_id')
            entities = device.get('data')

            sensor_data = [
                NestSensorData.from_entity(data=entity)
                for entity in entities
            ]

            tasks.add_task(self.group_device_sensor_data(
                device_id=device_id,
                sensor_data=sensor_data))

        results = await tasks.run()

        return results

    async def group_device_sensor_data(
        self,
        device_id: str,
        sensor_data: List[NestSensorData]
    ) -> NestSensorDataQueryResponse:

        df = self.__to_dataframe(
            sensor_data=sensor_data)

        logger.info(f'Uncollapsed row count: {device_id}: {len(sensor_data)}')

        grouped = df.groupby([df['timestamp'].dt.minute]).first()

        logger.info(f'Collapsed row count: {device_id}: {len(grouped)}')

        return NestSensorDataQueryResponse(
            device_id=device_id,
            data=grouped.to_dict(orient='records'))

    async def get_cached_group_sensor_data(
        self,
        device_id: str,
        key: str
    ):
        cache_key = CacheKey.nest_device_grouped_sensor_data(
            device_id=device_id,
            key=key)

        logger.info(f'Get device sensor data: {cache_key}')

        data = await self.__cache_client.get_json(
            key=cache_key)

    def __to_dataframe(
        self,
        sensor_data: List[NestSensorData]
    ):
        data = list()
        for entry in sensor_data:
            data.append({
                'key': entry.key,
                'degrees_celsius': entry.degrees_celsius,
                'degrees_fahrenheit': entry.degrees_fahrenheit,
                'humidity_percent': entry.humidity_percent,
                'diagnostics': entry.diagnostics,
                'timestamp': entry.get_timestamp_datetime()
            })

        return pd.DataFrame(data)

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

        for device in devices:
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

        return device_health

    async def poll_sensor_status(
        self
    ):
        logger.info(f'Polling sensor status')

        health = await self.get_sensor_info()
        results = list()

        for device in health:

            logger.info(f'Checking sensor health: {device.device_id}')

            # Sensor health
            is_unhealthy = (
                device.health.status != HealthStatus.Healthy
            )

            logger.info(f'Is unhealthy: {is_unhealthy}')

            if not is_unhealthy:
                continue

            logger.info(
                f'Sending unhealthy alert for device: {device.device_id}')

            body = self.__get_sensor_failure_email_message_body(
                device=device,
                elapsed_seconds=device.health.seconds_elapsed)

            message, endpoint = self.__email_gateway.get_email_request(
                recipient=ALERT_EMAIL_RECIPIENT,
                subject=ALERT_EMAIL_SUBJECT,
                body=body)

            logger.info(f'Dispatching event message: {message.to_dict()}')
            logger.info(f'Event endpoint: {endpoint}')

            await self.__event_service.dispatch_email_event(
                endpoint=endpoint,
                message=message.to_dict())

            result = SensorPollResult(
                device_id=device.device_id,
                is_healthy=not is_unhealthy)

            results.append(result)

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
