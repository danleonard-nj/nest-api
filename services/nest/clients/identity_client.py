from typing import Dict
from framework.clients.cache_client import CacheClientAsync
from framework.configuration.configuration import Configuration
from framework.exceptions.nulls import ArgumentNullException
from framework.logger.providers import get_logger
from framework.validators.nulls import none_or_whitespace
from httpx import AsyncClient

from domain.auth import AuthClientConfig
from domain.cache import CacheKey
from domain.exceptions import (AuthClientNotFoundException,
                               AuthTokenFailureException)
from utils.utils import fire_task

logger = get_logger(__name__)


class IdentityClient:
    def __init__(
        self,
        configuration: Configuration,
        http_client: AsyncClient,
        cache_client: CacheClientAsync
    ):
        ArgumentNullException.if_none(configuration, 'configuration')
        ArgumentNullException.if_none(cache_client, 'cache_client')

        self._azure_ad = configuration.ad_auth
        self._http_client = http_client
        self._cache_client = cache_client
        self._clients = dict()

        self._register_clients()

    def _register_clients(
        self
    ):
        logger.info(f'Registering auth client credential configs')

        self._clients = dict()
        for client in self._azure_ad.clients:
            self.add_client(client)

    def add_client(
        self,
        config: Dict
    ) -> None:

        client_name = config.get('name')
        logger.info(f'Parsing auth client config: {client_name}')

        auth_client = AuthClientConfig(
            data=config)

        # Register the auth client credentials
        self._clients.update({
            client_name: auth_client.to_dict()
        })

        logger.info(f'Client registered successfully: {client_name}')

    async def get_token(
        self,
        client_name: str,
        scope: str = None
    ) -> str:

        ArgumentNullException.if_none_or_whitespace(client_name, 'client_name')

        cache_key = CacheKey.auth_token(
            client=client_name,
            scope=scope)

        logger.info(f'Auth token cache key: {cache_key}')

        cached_token = await self._cache_client.get_cache(
            key=cache_key)

        # Return cached token
        if not none_or_whitespace(cached_token):
            logger.info(f'Cached token token for client: {client_name}: {cache_key}')
            return cached_token

        # Get the client credential request config
        client_credentials = self._clients.get(client_name)

        if client_credentials is None:
            raise AuthClientNotFoundException(
                client_name=client_name)

        # Set the scope on the request if it's provided
        if not none_or_whitespace(scope):
            logger.info(f'Client credential request scope: {scope}')
            client_credentials |= {
                'scope': scope
            }

        response = await self._http_client.post(
            url=self._azure_ad.identity_url,
            data=client_credentials)

        logger.info(f'Client token status: {client_name}: {response.status_code}')

        # Handle failure to fetch
        if response.is_error:
            logger.info(f'Auth token failure for client: {client_name}: {scope}')

            raise AuthTokenFailureException(
                client_name=client_name,
                status_code=response.status_code,
                message=response.text)

        content = response.json()
        token = content.get('access_token')

        logger.info(f'Token fetched from client: {token}')

        fire_task(
            self._cache_client.set_cache(
                key=cache_key,
                value=token,
                ttl=60))

        return token
