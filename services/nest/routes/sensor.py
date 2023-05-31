from datetime import datetime, timedelta

from framework.di.service_provider import ServiceProvider
from framework.logger.providers import get_logger
from quart import request

from domain.auth import AuthKey, AuthPolicy
from domain.rest import NestSensorDataRequest
from utils.meta import MetaBlueprint
from services.nest_service import NestService

logger = get_logger(__name__)

sensor_bp = MetaBlueprint('sensor_bp', __name__)


def default_start_timestamp():
    date = datetime.utcnow() - timedelta(days=7)
    return int(date.timestamp())


@sensor_bp.with_key_auth('/api/sensor/<sensor_id>/config', methods=['GET'], key_name=AuthKey.NestApiKey)
async def get_sensor_config(container: ServiceProvider):
    service: NestService = container.resolve(NestService)

    # TODO: Return sensor config


@sensor_bp.with_key_auth('/api/sensor', methods=['POST'], key_name=AuthKey.NestApiKey)
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

    start_timestamp = request.args.get(
        'start_timestamp',
        default_start_timestamp())

    return await service.get_sensor_data(
        start_timestamp=start_timestamp)


@sensor_bp.configure('/api/sensor/grouped', methods=['GET'], auth_scheme=AuthPolicy.Default)
async def get_grouped_sensor_data(container: ServiceProvider):
    service: NestService = container.resolve(NestService)

    start_timestamp = request.args.get(
        'start_timestamp',
        default_start_timestamp())

    return await service.get_grouped_sensor_data(
        start_timestamp=start_timestamp)


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
