from collections.abc import Iterable

from framework.logger import get_logger

from clients.email_gateway_client import EmailGatewayClient
from services.event_service import EventService

logger = get_logger(__name__)


class AlertService:
    def __init__(
        self,
        email_client: EmailGatewayClient,
        event_service: EventService
    ):
        self.__email_client = email_client
        self.__event_service = event_service

    async def send_alert(
        self,
        recipient: str,
        subject: str,
        body: str
    ) -> None:

        email_request, endpoint = self.__email_client.get_email_event_request(
            recipient=recipient,
            subject=subject,
            body=body)

        logger.info(
            f'Dispatching email event message: {email_request.to_dict()}')

        await self.__event_service.dispatch_email_event(
            endpoint=endpoint,
            message=email_request.to_dict())

    async def send_datatable_email(
        self,
        recipient: str,
        subject: str,
        data: list[dict] | Iterable[dict] | dict
    ) -> None:

        if not isinstance(data, Iterable):
            data = [data]

        email_request, endpoint = self.__email_client.get_datatable_email_request(
            recipient=recipient,
            subject=subject,
            data=data)

        logger.info(f'Sending email alert: {email_request.to_dict()}')
        logger.info(f'Endpoint: {endpoint}')

        await self.__event_service.dispatch_email_event(
            endpoint=endpoint,
            message=email_request.to_dict())
