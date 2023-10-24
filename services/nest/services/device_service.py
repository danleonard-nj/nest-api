
import asyncio
from framework.logger import get_logger

from framework.clients.cache_client import CacheClientAsync
from data.nest_sensor_repository import NestDeviceRepository
from domain.cache import CacheKey
from domain.nest import NestSensorDevice

logger = get_logger(__name__)


class NestDeviceService:
    def __init__(
        self,
        device_repository: NestDeviceRepository,
        cache_client: CacheClientAsync
    ):
        self.__device_repository = device_repository
        self.__cache_client = cache_client

    async def get_devices_by_ids(
        self,
        device_ids: list
    ):
        entities = await self.__device_repository.get_devices(
            device_ids=device_ids)

        devices = [NestSensorDevice.from_entity(data=entity)
                   for entity in entities]

        return devices

    async def get_devices(
        self
    ) -> list[NestSensorDevice]:

        key = CacheKey.nest_devices()

        logger.info(f'Get cached devices: {key}')

        entities = await self.__cache_client.get_json(
            key=key)

        if entities is not None and any(entities):
            logger.info(f'Returning devices from cache: {key}')

            # Return cached devices
            return [NestSensorDevice.from_entity(data=entity)
                    for entity in entities]

        # Fetch devices from database
        logger.info(f'Fetching devices from database: {key}')
        entities = await self.__device_repository.get_all()

        # Fire and forget the cache task
        asyncio.create_task(
            self.__cache_client.set_json(
                key=key,
                value=entities))

        return [NestSensorDevice.from_entity(data=entity)
                for entity in entities]

    async def get_device(
        self,
        device_id: str
    ):
        key = CacheKey.nest_device(
            sensor_id=device_id)

        logger.info(f'Get cached device: {key}')

        entity = await self.__cache_client.get_json(
            key=key)

        if entity is not None:
            logger.info(f'Returning device from cache: {key}')

            # Return the cached device
            return NestSensorDevice.from_entity(
                data=entity)

        # Fetch device from database
        entity = await self.__device_repository.get({
            'device_id': device_id
        })

        # Device not found
        if entity is None:
            raise Exception(f'No device found for sensor_id: {device_id}')

        # Fire and forget the cache task
        asyncio.create_task(
            self.__cache_client.set_json(
                key=key,
                value=entity))

        device = NestSensorDevice.from_entity(
            data=entity)

        return device
