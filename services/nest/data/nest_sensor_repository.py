from framework.mongo.mongo_repository import MongoRepositoryAsync
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.collection import Collection


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
        result = self.collection.find({
            'sensor_id': {
                '$in': device_ids
            },
            'timestamp': {
                '$gte': int(start_timestamp)
            }
        })

        return await result.to_list(
            length=None)

    async def get_by_device(
        self,
        device_id: str,
        start_timestamp: int
    ):
        result = self.collection.aggregate([
            {
                '$match': {
                    'sensor_id': device_id,
                    'timestamp': {
                        '$gte': int(start_timestamp)
                    }
                }
            }
        ])

        return await result.to_list(
            length=None)

    async def get_top_sensor_record(
        self,
        sensor_id: str
    ):
        query_filter = {
            'sensor_id': sensor_id
        }

        return await self.collection.find_one(
            filter=query_filter,
            sort=[('timestamp', -1)])

    async def purge_records_before_cutoff(
        self,
        cutoff_timestamp: int
    ):
        return await self.collection.delete_many({
            'timestamp': {
                '$lte': cutoff_timestamp
            }
        })


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
        return await self.collection.find({
            'device_id': {
                '$in': device_ids
            }
        }).to_list(
            length=None)
