from domain.queries import (GetByDeviceQuery, GetDevicesQuery,
                            GetSensorDataByDevicesQuery, GetTopSensorRecordQuery,
                            PurgeRecordsBeforeCutoffQuery)
from domain.mongo import Queryable
from framework.logger import get_logger
from framework.mongo.mongo_repository import MongoRepositoryAsync
from httpx import get
from motor.motor_asyncio import AsyncIOMotorClient

logger = get_logger(__name__)


class NestSensorRepository(MongoRepositoryAsync):
    def __init__(
        self,
        client: AsyncIOMotorClient
    ):
        super().__init__(
            client=client,
            database='Nest',
            collection='Sensor')

    async def get_sensor_data_by_devices(
        self,
        device_ids: list[str],
        start_timestamp: int
    ):
        query = GetSensorDataByDevicesQuery(
            device_ids=device_ids,
            start_timestamp=start_timestamp)

        return await (self.collection
                      .find(query.get_query())
                      .to_list(length=None))

    async def get_by_device(
        self,
        device_id: str,
        start_timestamp: int
    ):
        query = GetByDeviceQuery(
            device_id=device_id,
            start_timestamp=start_timestamp)

        return await (self.collection
                      .aggregate(query.get_pipeline())
                      .to_list(length=None))

    async def get_top_sensor_record(
        self,
        sensor_id: str
    ):
        query = GetTopSensorRecordQuery(
            sensor_id=sensor_id)

        return await (self.collection.find_one(
            filter=query.get_query(),
            sort=query.get_sort()))

    async def purge_records_before_cutoff(
        self,
        cutoff_timestamp: int
    ):
        query = PurgeRecordsBeforeCutoffQuery(
            cutoff_timestamp=cutoff_timestamp)

        return await self.collection.delete_many(
            query.get_query())


class NestDeviceRepository(MongoRepositoryAsync):
    def __init__(
        self,
        client: AsyncIOMotorClient
    ):
        super().__init__(
            client=client,
            database='Nest',
            collection='Device')

    async def get_devices(
        self,
        device_ids: list[str]
    ):
        query = GetDevicesQuery(
            device_ids=device_ids)

        return await (self.collection
                      .find(query.get_query())
                      .to_list(length=None))
