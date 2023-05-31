import json
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
from domain.nest import (HealthStatus, NestSensorData,
                         NestSensorDataQueryResult, NestSensorDevice,
                         NestThermostat, SensorDataPurgeResult, SensorHealth,
                         SensorHealthStats, SensorPollResult)
from domain.rest import NestSensorDataRequest
from services.event_service import EventService
from utils.utils import DateTimeUtil

logger = get_logger(__name__)

SENSOR_UNHEALTHY_SECONDS = 4
DEFAULT_PURGE_DAYS = 90


class NestService:
    def __init__(
        self,
        configuration: Configuration,
        nest_client: NestClient,
        sensor_repository: NestSensorRepository,
        device_repository: NestDeviceRepository,
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
        self.__device_repository = device_repository
        self.__event_service = event_service
        self.__email_gateway = email_gateway
        self.__cache_client = cache_client

    async def get_thermostat(
        self
    ) -> NestThermostat:

        data = await self.__nest_client.get_thermostat()

        thermostat = NestThermostat.from_json_object(
            data=data,
            thermostat_id=self.__thermostat_id)

        return thermostat

    async def log_sensor_data(
        self,
        sensor_request: NestSensorDataRequest
    ) -> NestSensorData:

        sensor = await self.__get_sensor(
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
            timestamp=DateTimeUtil.timestamp())

        logger.info(f'Capturing sensor data: {sensor_data}')

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

        await self.__event_service.dispatch_email_event(
            endpoint=endpoint,
            message=email_request.to_dict())

        return SensorDataPurgeResult(
            deleted=result.deleted_count)

    async def get_sensor_data(
        self,
        start_timestamp: int
    ) -> List[Dict[str, List[NestSensorData]]]:

        logger.info(f'Get sensor data: {start_timestamp}')
        device_entities = await self.__device_repository.get_all()

        devices = [
            NestSensorDevice.from_entity(data=entity)
            for entity in device_entities
        ]

        results = list()

        for device in devices:
            logger.info(f'Fetching data for device: {device.device_id}')
            entities = await self.__sensor_repository.get_by_device(
                device_id=device.device_id,
                start_timestamp=start_timestamp)

            data = [NestSensorData.from_entity(data=entity)
                    for entity in entities]

            results.append({
                'device_id': device.device_id,
                'data': data
            })

        return results

    async def get_grouped_sensor_data(
        self,
        start_timestamp: int
    ):
        data = await self.get_sensor_data(
            start_timestamp=start_timestamp)

        # with open(r'C:\temp\sensor_data.json', 'w') as file:
        #     for device in data:
        #         device['data'] = [x.to_dict() for x in device['data']]

        #     file.write(json.dumps(data, default=str, indent=True))

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
    ) -> NestSensorDataQueryResult:

        df = self.__to_dataframe(
            sensor_data=sensor_data)

        logger.info(f'Uncollapsed row count: {device_id}: {len(sensor_data)}')

        grouped = df.groupby([df['timestamp'].dt.minute]).first()

        logger.info(f'Collapsed row count: {device_id}: {len(grouped)}')

        return NestSensorDataQueryResult(
            device_id=device_id,
            data=grouped.to_dict(orient='records'))

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
                'timestamp': entry.get_timestamp_datetime()
            })

        return pd.DataFrame(data)

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

    async def __get_top_sensor_record(
        self,
        device_id: str
    ) -> NestSensorData:

        last_entity = await self.__sensor_repository.get_top_sensor_record(
            sensor_id=device_id)

        if last_entity is None:
            return None

        last_record = NestSensorData.from_entity(
            data=last_entity)

        return last_record

    async def __get_all_devices(
        self
    ) -> List[NestSensorDevice]:

        entities = await self.__device_repository.get_all()

        devices = [NestSensorDevice.from_entity(data=entity)
                   for entity in entities]

        return devices

    def __get_health_status(
        self,
        record: NestSensorData
    ) -> Tuple[str, int]:

        now = DateTimeUtil.timestamp()

        seconds_elapsed = now - record.timestamp
        logger.info(f'Seconds elapsed: {seconds_elapsed}')

        # Health status (healthy/unhealthy)
        health_status = (
            HealthStatus.Unhealthy
            if seconds_elapsed >= SENSOR_UNHEALTHY_SECONDS
            else HealthStatus.Healthy
        )

        return (
            health_status,
            seconds_elapsed
        )

    async def get_sensor_info(
        self
    ) -> List[SensorHealth]:

        logger.info(f'Getting sensor info')
        devices = await self.__get_all_devices()

        device_health = list()

        for device in devices:
            logger.info(f'Calculating health stats: {device.device_id}')

            last_record = await self.__get_top_sensor_record(
                device_id=device.device_id)

            health_status, seconds_elapsed = self.__get_health_status(
                record=last_record)

            stats = SensorHealthStats(
                status=health_status,
                last_contact=last_record.timestamp,
                seconds_elapsed=seconds_elapsed)

            health = SensorHealth(
                device_id=device.device_id,
                device_name=device.device_name,
                health=stats,
                data=last_record)

            device_health.append(health)

        return device_health

    async def __get_sensor(
        self,
        device_id: str
    ) -> NestSensorDevice:

        entity = await self.__device_repository.get({
            'device_id': device_id
        })

        if entity is None:
            return None

        device = NestSensorDevice.from_entity(
            data=entity)

        return device

    async def poll_sensor_status(
        self
    ):
        health = await self.get_sensor_info()
        results = list()

        for device in health:

            # Sensor health
            is_unhealthy = (
                device.health.status != HealthStatus.Healthy
            )

            if not is_unhealthy:
                continue

            logger.info(
                f'Sending unhealthy alert for device: {device.device_id}')

            body = self.__get_sensor_failure_email_message_body(
                device=device,
                elapsed_seconds=device.health.seconds_elapsed)

            message, endpoint = self.__email_gateway.get_email_request(
                recipient='dcl525@gmail.com',
                subject='ESP8266 Sensor Failure',
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
        msg += f'Count: {deleted_count}\n'
