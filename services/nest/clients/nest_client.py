import asyncio
from typing import Dict

from framework.clients.cache_client import CacheClientAsync
from framework.configuration import Configuration
from framework.logger import get_logger
from framework.validators.nulls import none_or_whitespace
from httpx import AsyncClient

from domain.cache import CacheKey
from domain.exceptions import NestAuthorizationFailureException
from domain.rest import AuthorizationRequest

logger = get_logger(__name__)


class NestClient:
    def __init__(
        self,
        configuration: Configuration,
        http_client: AsyncClient,
        cache_client: CacheClientAsync

    ):
        self._base_url = configuration.nest.get('base_url')
        self._token_url = configuration.nest.get('token_url')

        self._device_id = configuration.nest.get('thermostat_id')
        self._project_id = configuration.nest.get('project_id')

        self._client_id = configuration.nest.get('client_id')
        self._client_secret = configuration.nest.get('client_secret')
        self._refresh_token = configuration.nest.get('refresh_token')

        self._http_client = http_client
        self._cache_client = cache_client

    async def get_token(
        self
    ):
        key = CacheKey.google_nest_auth_token()

        logger.info(f'Nest auth token key: {key}')

        token = await self._cache_client.get_cache(
            key=key)

        if not none_or_whitespace(token):
            logger.info(f'Using cached nest auth token: {token}')
            return token

        logger.info(f'Fetching token from auth client')
        token = await self._fetch_token()

        # Cache the Nest auth token
        asyncio.create_task(
            self._cache_client.set_cache(
                key=key,
                value=token,
                ttl=60))

        logger.info(f'Token fetched: {token}')
        return token

    async def get_thermostat(
        self
    ) -> dict:
        headers = await self._get_headers()

        logger.info(f'Getting thermostat: {self._device_id}')

        endpoint = f'{self._base_url}/v1/enterprises/{self._project_id}/devices/{self._device_id}'
        logger.info(f'Endpoint: {endpoint}')

        response = await self._http_client.get(
            url=endpoint,
            headers=headers)

        logger.info(f'Thermostat fetched: {response.status_code}')
        return response.json()

    async def execute_command(
        self,
        command: dict
    ):
        logger.info(f'Executing command: {command}')
        headers = await self._get_headers()

        endpoint = f'{self._base_url}/v1/enterprises/{self._project_id}/devices/{self._device_id}:executeCommand'
        logger.info(f'Endpoint: {endpoint}')

        response = await self._http_client.post(
            url=endpoint,
            headers=headers,
            json=command)

        logger.info(f'Command executed: {response.status_code}')

        return response.json()

    async def _get_headers(
        self
    ) -> Dict:

        token = await self.get_token()

        return {
            'Authorization': f'Bearer {token}'
        }

    async def _fetch_token(
        self
    ):
        payload = AuthorizationRequest(
            client_id=self._client_id,
            client_secret=self._client_secret,
            grant_type='refresh_token',
            refresh_token=self._refresh_token)

        response = await self._http_client.post(
            url=self._token_url,
            data=payload.to_dict())

        if not response.is_success:
            logger.info(f'Failed to fetch nest token: {response.status_code}')
            raise NestAuthorizationFailureException()

        logger.info(f'Nest auth token response: {response.status_code}')

        content = response.json()
        return content.get('access_token')
