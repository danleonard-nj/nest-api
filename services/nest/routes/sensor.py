from datetime import datetime, timedelta

from framework.di.service_provider import ServiceProvider
from framework.logger.providers import get_logger
from quart import request

from domain.auth import AuthPolicy
from domain.rest import NestSensorDataRequest, NestSensorLogRequest
from services.nest_service import NestService
from utils.meta import MetaBlueprint

API_KEY_NAME = 'nest-sensor-api-key'

logger = get_logger(__name__)

sensor_bp = MetaBlueprint('sensor_bp', __name__)


def default_start_timestamp():
    date = datetime.utcnow() - timedelta(days=7)
    return int(date.timestamp())


@sensor_bp.configure('/api/sensor/purge', methods=['POST'], auth_scheme=AuthPolicy.Default)
async def post_sensor_purge(container: ServiceProvider):
    service: NestService = container.resolve(NestService)

    return await service.purge_sensor_data()


@sensor_bp.with_key_auth('/api/sensor/log', methods=['POST'], key_name=API_KEY_NAME)
async def post_sensor_log(container: ServiceProvider):
    service: NestService = container.resolve(NestService)

    body = await request.get_json()

    req = NestSensorLogRequest(
        data=body)

    return await service.log_message(
        req=req)


@sensor_bp.with_key_auth('/api/sensor', methods=['POST'], key_name=API_KEY_NAME)
async def post_sensor_data(container: ServiceProvider):
    service: NestService = container.resolve(NestService)

    body = await request.get_json()

    sensor_request = NestSensorDataRequest(
        data=body)

    return await service.log_sensor_data(
        sensor_request=sensor_request)


@sensor_bp.configure('/api/sensor', methods=['GET'], auth_scheme=AuthPolicy.Default)
async def get_sensor_data(container: ServiceProvider):
    service: NestService = container.resolve(NestService)

    hours_back = request.args.get(
        'hours_back', 1)

    params = request.args.to_dict(flat=False)

    devices = (
        params.get('device_id', [])
        if 'devices' in params else []
    )

    sample = request.args.get('sample', '5min')

    return await service.get_sensor_data(
        hours_back == hours_back,
        device_ids=devices,
        sample=sample)


@sensor_bp.configure('/api/sensor/<sensor_id>', methods=['GET'], auth_scheme=AuthPolicy.Default)
async def get_sensor_id(container: ServiceProvider, sensor_id: str):
    service: NestService = container.resolve(NestService)

    start_timestamp = request.args.get(
        'start_timestamp',
        default_start_timestamp())

    params = request.args.to_dict(flat=False)
    devices = params.get('device_id', [])

    return await service.get_sensor_data(
        start_timestamp=start_timestamp,
        device_ids=devices)


# @sensor_bp.configure('/api/sensor/grouped', methods=['GET'], auth_scheme=AuthPolicy.Default)
# async def get_grouped_sensor_data(container: ServiceProvider):
#     service: NestService = container.resolve(NestService)

#     start_timestamp = request.args.get(
#         'start_timestamp',
#         default_start_timestamp())

#     return await service.get_grouped_sensor_data(
#         start_timestamp=start_timestamp)


@sensor_bp.configure('/api/sensor/info', methods=['GET'], auth_scheme=AuthPolicy.Default)
async def get_sensor_info(container: ServiceProvider):
    service: NestService = container.resolve(NestService)

    start_timestamp = request.args.get(
        'start_timestamp',
        default_start_timestamp())

    return await service.get_sensor_info()


@sensor_bp.configure('/api/sensor/info/poll', methods=['POST'], auth_scheme=AuthPolicy.Default)
async def get_sensor_info_poll(container: ServiceProvider):
    service: NestService = container.resolve(NestService)

    return await service.poll_sensor_status()
