import json

from framework.crypto.hashing import sha256


def generate_key(items):
    return sha256(
        data=json.dumps(
            items,
            default=str
        )
    )


def parse(value, enum_type):
    if isinstance(value, str):
        return enum_type(value)
    return value
