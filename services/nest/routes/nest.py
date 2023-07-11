from datetime import datetime, timedelta

from framework.di.service_provider import ServiceProvider
from framework.logger.providers import get_logger
from framework.rest.blueprints.meta import MetaBlueprint
from quart import request

from clients.nest_client import NestClient
from domain.auth import AuthPolicy
from domain.rest import NestSensorDataRequest, NestSensorLogRequest
from services.nest_service import NestService

logger = get_logger(__name__)

nest_bp = MetaBlueprint('nest_bp', __name__)


def default_start_timestamp():
    date = datetime.utcnow() - timedelta(days=7)
    return int(date.timestamp())


@nest_bp.configure('/api/nest/auth', methods=['GET'], auth_scheme=AuthPolicy.Default)
async def get_auth_creds(container: ServiceProvider):
    service: NestClient = container.resolve(NestClient)

    token = await service.get_token()

    return {
        'token': token
    }


@nest_bp.configure('/api/nest/thermostat', methods=['GET'], auth_scheme=AuthPolicy.Default)
async def get_thermostat(container: ServiceProvider):
    service: NestService = container.resolve(NestService)

    return await service.get_thermostat()



