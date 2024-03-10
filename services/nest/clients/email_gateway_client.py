from typing import Any

from clients.identity_client import IdentityClient
from domain.auth import ClientScope
from domain.email_gateway import EmailGatewayRequest
from domain.rest import AuthorizationHeader
from framework.configuration import Configuration
from framework.logger.providers import get_logger
from httpx import AsyncClient

logger = get_logger(__name__)


class EmailGatewayClient:
    def __init__(
        self,
        configuration: Configuration,
        identity_client: IdentityClient,
        http_client: AsyncClient
    ):
        self._http_client = http_client
        self._identity_client = identity_client
        self._base_url = configuration.gateway.get(
            'email_gateway_base_url')

    async def send_email(
        self,
        subject: str,
        recipient: str,
        message: str
    ):
        endpoint = f'{self._base_url}/api/email/send'
        logger.info(f'Endpoint: {endpoint}')

        content = EmailGatewayRequest(
            recipient=recipient,
            subject=subject,
            body=message)

        headers = await self._get_auth_headers()

        response = await self._http_client.post(
            url=endpoint,
            headers=headers,
            json=content.to_dict())

        logger.info(f'Status code: {response.status_code}')

        if response.status_code != 200:
            logger.info(f'Failed to send email: {response.text}')

        return response.json()

    async def send_datatable_email(
        self,
        recipient: str,
        subject: str,
        data: list[dict]
    ):
        logger.info(f'Sending datatable email')

        endpoint = f'{self._base_url}/api/email/datatable'
        logger.info(f'Endpoint: {endpoint}')

        content = EmailGatewayRequest(
            recipient=recipient,
            subject=subject,
            table=data)

        headers = await self._get_auth_headers()

        response = await self._http_client.post(
            url=endpoint,
            headers=headers,
            json=content.to_dict())

        logger.info(f'Response status: {response.status_code}')
        return response.json()

    def get_datatable_email_request(
        self,
        recipient: str,
        subject: str,
        data: list[dict]
    ):
        endpoint = f'{self._base_url}/api/email/datatable'
        logger.info(f'Endpoint: {endpoint}')

        content = EmailGatewayRequest(
            recipient=recipient,
            subject=subject,
            table=data)

        return content, endpoint

    def get_email_event_request(
        self,
        recipient: str,
        subject: str,
        body: str
    ):
        endpoint = f'{self._base_url}/api/email/send'
        logger.info(f'Endpoint: {endpoint}')

        content = EmailGatewayRequest(
            recipient=recipient,
            subject=subject,
            body=body)

        logger.info(f'Email request: {content.to_dict()}')

        return content, endpoint

    async def send_json_email(
        self,
        recipient: str,
        subject: str,
        data: Any
    ) -> dict:
        endpoint = f'{self._base_url}/api/email/json'
        logger.info(f'Endpoint: {endpoint}')

        content = EmailGatewayRequest(
            recipient=recipient,
            subject=subject,
            json=data)

        headers = await self._get_auth_headers()

        response = await self._http_client.post(
            url=endpoint,
            headers=headers,
            json=content.to_dict())

        logger.info(f'Response status: {response.status_code}')
        return response.json()

    async def _get_auth_headers(
        self
    ) -> dict[str, str]:
        logger.info(f'Fetching email gateway auth token')

        token = await self._identity_client.get_token(
            client_name='kube-tools-api',
            scope=ClientScope.EmailGatewayApi)

        auth_headers = AuthorizationHeader(
            token=token)

        return auth_headers.to_dict()
