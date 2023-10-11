from datetime import datetime, timedelta

from framework.di.service_provider import ServiceProvider
from framework.logger.providers import get_logger
from framework.rest.blueprints.meta import MetaBlueprint

from clients.nest_client import NestClient
from domain.auth import AuthPolicy
from domain.rest import NestTokenResponse
from services.nest_service import NestService

logger = get_logger(__name__)

nest_bp = MetaBlueprint('nest_bp', __name__)


def default_start_timestamp():
    date = datetime.utcnow() - timedelta(days=7)
    return int(date.timestamp())


@nest_bp.configure('/api/auth', methods=['GET'], auth_scheme=AuthPolicy.Default)
async def get_auth_creds(container: ServiceProvider):
    service: NestClient = container.resolve(NestClient)

    token = await service.get_token()

    return NestTokenResponse(
        token=token)


@nest_bp.configure('/api/thermostat', methods=['GET'], auth_scheme=AuthPolicy.Default)
async def get_thermostat(container: ServiceProvider):
    service: NestService = container.resolve(NestService)

    return await service.get_thermostat()


@nest_bp.configure('/api/thermostat/capture', methods=['POST'], auth_scheme=AuthPolicy.Default)
async def capture_thermostat(container: ServiceProvider):
    service: NestService = container.resolve(NestService)

    return await service.capture_thermostat_history()
