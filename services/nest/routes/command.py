from framework.di.service_provider import ServiceProvider
from framework.logger.providers import get_logger
from framework.rest.blueprints.meta import MetaBlueprint
from quart import request

from domain.auth import AuthPolicy
from domain.rest import NestCommandRequest

logger = get_logger(__name__)

command_bp = MetaBlueprint('command_bp', __name__)


@command_bp.configure('/api/command', methods=['POST'], auth_scheme=AuthPolicy.Default)
async def post_command(container: ServiceProvider):
    service: NestCommandService = container.resolve(NestCommandService)

    body = await request.get_json()

    command_request = NestCommandRequest(
        data=body)

    return await service.handle_command(
        command_type=command_request.command_type,
        params=command_request.params)
