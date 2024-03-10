from clients.identity_client import IdentityClient
from domain.auth import ClientScope
from framework.configuration import Configuration
from framework.logger.providers import get_logger
from httpx import AsyncClient

logger = get_logger(__name__)


class KasaClient:
    def __init__(
        self,
        configuration: Configuration,
        http_client: AsyncClient,
        identity_client: IdentityClient
    ):
        self._base_url = configuration.kasa.get('base_url')

        self._http_client = http_client
        self._identity_client = identity_client

    async def _get_headers(
        self
    ):
        token = await self._identity_client.get_token(
            client_name='nest-api',
            scope=ClientScope.KasaApi)

        return {
            'Authorization': f'Bearer {token}'
        }

    async def run_scene(
        self,
        scene_id: str
    ):
        logger.info(f'Running scene {scene_id}')

        headers = await self._get_headers()
        logger.info(f'Headers: {headers}')

        endpoint = f'{self._base_url}/scene/{scene_id}/run'
        logger.info(f'Endpoint: {endpoint}')

        response = await self._http_client.post(
            url=endpoint,
            headers=headers)

        logger.info(f'Response status: {response.status_code}')

        return (
            response.status_code,
            response.json()
        )
