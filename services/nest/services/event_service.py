from framework.exceptions.nulls import ArgumentNullException
from framework.logger import get_logger

from clients.event_client import EventClient
from clients.identity_client import IdentityClient
from domain.auth import ClientScope
from domain.events import SendEmailEvent

logger = get_logger(__name__)


class EventService:
    def __init__(
        self,
        event_client: EventClient,
        identity_client: IdentityClient
    ):
        self.__event_client = event_client
        self.__identity_client = identity_client

    async def dispatch_email_event(
        self,
        endpoint: str,
        message: str
    ) -> None:

        ArgumentNullException.if_none_or_whitespace(endpoint, 'endpoint')
        ArgumentNullException.if_none_or_whitespace(message, 'message')

        logger.info(f'Emit email notification event: {endpoint}')

        token = await self.__identity_client.get_token(
            client_name='kube-tools-api',
            scope=ClientScope.EmailGatewayApi)

        event = SendEmailEvent(
            body=message,
            endpoint=endpoint,
            token=token)

        logger.info(f'Email event message: {event.to_dict()}')

        self.__event_client.send_message(
            event.to_service_bus_message())
