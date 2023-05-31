import hashlib
import json
from typing import Any
import uuid


def generate_uuid(data: Any):
    parsed = json.dumps(data, default=str)
    hashed = hashlib.md5(parsed.encode())
    return str(uuid.UUID(hashed.hexdigest()))


class CacheKey:
    @staticmethod
    def google_nest_auth_token() -> str:
        return 'google-nest-auth-token'

    @staticmethod
    def nest_device_grouped_sensor_data(
        device_id,
        key
    ) -> str:
        hash_key = generate_uuid([
            device_id,
            key
        ])

        return f'google-nest-gsd-{hash_key}'
