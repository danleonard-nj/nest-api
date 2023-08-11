from framework.di.service_provider import ServiceProvider
from framework.logger.providers import get_logger
from framework.rest.blueprints.meta import MetaBlueprint
from quart import request

from domain.auth import AuthPolicy
from services.integration_service import NestIntegrationService

logger = get_logger(__name__)

integration_bp = MetaBlueprint('integration_bp', __name__)


def get_integration_event_params():
    start_timestamp = request.args.get('start_timestamp')
    end_timestamp = request.args.get('end_timestamp')
    max_results = request.args.get('max_results')
    sensor_id = request.args.get('sensor_id')

    return dict(
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        sensor_id=sensor_id,
        max_results=max_results
    )


@integration_bp.configure('/api/integration/events', methods=['GET'], auth_scheme=AuthPolicy.Default)
async def get_integration_events(container: ServiceProvider):
    service: NestIntegrationService = container.resolve(NestIntegrationService)

    params = get_integration_event_params()

    return await service.get_integration_events(
        **params)
