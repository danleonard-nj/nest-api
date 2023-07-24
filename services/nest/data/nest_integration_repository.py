from typing import Any, Dict

from framework.mongo.mongo_repository import MongoRepositoryAsync
from motor.motor_asyncio import AsyncIOMotorClient


class NestIntegrationRepository(MongoRepositoryAsync):
    def __init__(
        self,
        client: AsyncIOMotorClient
    ):
        super().__init__(
            client=client,
            database='Nest',
            collection='Integration')

    async def get_latest_integation_event_by_sensor(
        self,
        sensor_id: str
    ) -> Dict[str, Any]:

        query_filter = {
            'sensor_id': sensor_id
        }

        return await self.collection.find_one(
            filter=query_filter,
            sort=[('timestamp', -1)]
        )
