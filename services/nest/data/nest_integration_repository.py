from typing import Any

from framework.mongo.mongo_repository import MongoRepositoryAsync
from motor.motor_asyncio import AsyncIOMotorClient

from domain.mongo import Queryable


class GetIntegarationEventsQuery(Queryable):
    def __init__(
        self,
        start_timestamp: int,
        end_timestamp: int,
        sensor_id: str = None
    ):
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.sensor_id = sensor_id

    def get_query(self) -> dict[str, any]:
        query_filter = {
            'timestamp': {
                '$gt': self.start_timestamp,
                '$lte': self.end_timestamp
            }
        }

        if self.sensor_id is not None:
            query_filter['sensor_id'] = self.sensor_id

        return query_filter


class GetLatestIntegrationEventBySensorQuery(Queryable):
    def __init__(
        self,
        sensor_id: str
    ):
        self.sensor_id = sensor_id

    def get_query(self) -> dict[str, any]:
        query_filter = {
            'sensor_id': self.sensor_id
        }

        return query_filter


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

        # query_filter = {
        #     'timestamp': {
        #         '$gt': start_timestamp,
        #         '$lte': end_timestamp
        #     }
        # }

        # if sensor_id is not None:
        #     query_filter['sensor_id'] = sensor_id

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

        # query_filter = {
        #     'sensor_id': sensor_id
        # }

        query = GetLatestIntegrationEventBySensorQuery(
            sensor_id=sensor_id)

        return await self.collection.find_one(
            filter=query.get_query(),
            sort=[('timestamp', -1)]
        )
