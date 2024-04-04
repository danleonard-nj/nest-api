from typing import Any
from framework.mongo.mongo_repository import MongoRepositoryAsync
from motor.motor_asyncio import AsyncIOMotorClient

from domain.queries import GetIntegarationEventsQuery, GetLatestIntegrationEventBySensorQuery


class NestIntegrationRepository(MongoRepositoryAsync):
    def __init__(
        self,
        client: AsyncIOMotorClient
    ):
        super().__init__(
            client=client,
            database='Nest',
            collection='Integration')

    async def get_integration_events(
        self,
        start_timestamp: int,
        end_timestamp: int,
        sensor_id: str = None
    ) -> list[dict[str, any]]:

        query = GetIntegarationEventsQuery(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            sensor_id=sensor_id)

        results = await (self.collection
                         .find(query.get_query())
                         .to_list(length=None))

        return results

    async def get_latest_integation_event_by_sensor(
        self,
        sensor_id: str
    ) -> dict[str, Any]:

        query = GetLatestIntegrationEventBySensorQuery(
            sensor_id=sensor_id)

        return await self.collection.find_one(
            filter=query.get_query(),
            sort=query.get_sort())
