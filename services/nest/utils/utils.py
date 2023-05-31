import hashlib
import json
import time
from typing import Union
import uuid


class DateTimeUtil:
    @staticmethod
    def timestamp() -> int:
        return int(time.time())


class KeyUtils:
    @staticmethod
    def create_uuid(**kwargs) -> str:
        '''
        Create a UUID based on the contents of kwargs
        '''

        digest = hashlib.md5(json.dumps(
            kwargs,
            default=str).encode())

        return str(uuid.UUID(digest.hexdigest()))


def to_celsius(
    degrees_fahrenheit: Union[int, float],
    round_digits: int = 2
) -> Union[int, float]:
    '''
    Convert degrees Fahrenheit to degrees Celsius
    '''

    value = (degrees_fahrenheit - 32) * (5/9)
    return round(value, round_digits)
