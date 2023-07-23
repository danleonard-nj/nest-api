import json
from framework.crypto.hashing import sha256


def apply(items, func):
    return list(map(func, items))


def get_map(items: list, key: str, is_dict: bool = True):
    if is_dict:
        return {
            item.get(key): item
            for item in items
        }

    else:
        return {
            getattr(item, key): item
            for item in items
        }


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
