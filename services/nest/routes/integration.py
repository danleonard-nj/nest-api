from framework.di.service_provider import ServiceProvider
from framework.logger.providers import get_logger
from framework.rest.blueprints.meta import MetaBlueprint
from quart import request

from domain.auth import AuthPolicy
from services.integration_service import NestIntegrationService

logger = get_logger(__name__)

integration_bp = MetaBlueprint('integration_bp', __name__)


def get_integration_event_params():

    return dict(

    )


@integration_bp.configure('/api/integration/events', methods=['GET'], auth_scheme=AuthPolicy.Default)
async def get_integration_events(container: ServiceProvider):
    service: NestIntegrationService = container.resolve(NestIntegrationService)

    days_back = request.args.get('days_back', 1)
    sensor_id = request.args.get('sensor_id')

    return await service.get_integration_events(
        days_back=days_back,
        sensor_id=sensor_id)
