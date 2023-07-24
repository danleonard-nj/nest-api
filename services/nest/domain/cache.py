import hashlib
import json
from typing import Any
import uuid

from utils.utils import KeyUtils


def generate_uuid(data: Any):
    parsed = json.dumps(data, default=str)
    hashed = hashlib.md5(parsed.encode())
    return str(uuid.UUID(hashed.hexdigest()))


class CacheKey:
    @staticmethod
    def google_nest_auth_token() -> str:
        return 'nest-google-auth-token'

    @staticmethod
    def active_thermostat_mode() -> str:
        return 'nest-active-thermostat-mode'

    @staticmethod
    def nest_devices() -> str:
        return 'nest-devices'

    @staticmethod
    def nest_device(sensor_id) -> str:
        return f'nest-device-sensor-id-{sensor_id}'

    @staticmethod
    def auth_token(**kwargs) -> str:
        key = KeyUtils.create_uuid(**kwargs)
        return f'nest-auth-token-{key}'

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
