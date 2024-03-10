from domain.mongo import Queryable


class GetSensorDataByDevicesQuery(Queryable):
    def __init__(
        self,
        device_ids: list[str],
        start_timestamp: int
    ):
        self.device_ids = device_ids
        self.start_timestamp = start_timestamp

    def get_query(self) -> dict[str, any]:
        query_filter = {
            'sensor_id': {
                '$in': self.device_ids
            },
            'timestamp': {
                '$gte': int(self.start_timestamp)
            }
        }

        return query_filter


class GetByDeviceQuery(Queryable):
    def __init__(
        self,
        device_id: str,
        start_timestamp: int
    ):
        self.device_id = device_id
        self.start_timestamp = start_timestamp

    def get_pipeline(
        self
    ) -> dict:
        return [
            {
                '$match': {
                    'sensor_id': self.device_id,
                    'timestamp': {
                        '$gte': int(self.start_timestamp)
                    }
                }
            }
        ]


class GetTopSensorRecordQuery(Queryable):
    def __init__(
        self,
        sensor_id: str
    ):
        self.sensor_id = sensor_id

    def get_query(
        self
    ) -> dict[str, any]:
        query_filter = {
            'sensor_id': self.sensor_id
        }

        return query_filter

    def get_sort(
        self
    ) -> list:
        return [('timestamp', -1)]


class PurgeRecordsBeforeCutoffQuery(Queryable):
    def __init__(
        self,
        cutoff_timestamp: int
    ):
        self.cutoff_timestamp = cutoff_timestamp

    def get_query(
        self
    ) -> dict[str, any]:
        query_filter = {
            'timestamp': {
                '$lte': self.cutoff_timestamp
            }
        }

        return query_filter


class GetDevicesQuery(Queryable):
    def __init__(
        self,
        device_ids: list[str]
    ):
        self.device_ids = device_ids

    def get_query(
        self
    ) -> dict[str, any]:
        query_filter = {
            'device_id': {
                '$in': self.device_ids
            }
        }

        return query_filter
