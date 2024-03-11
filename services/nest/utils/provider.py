from framework.auth.azure import AzureAd
from framework.auth.configuration import AzureAdConfiguration
from framework.clients.cache_client import CacheClientAsync
from framework.clients.feature_client import FeatureClientAsync
from framework.configuration.configuration import Configuration
from framework.di.service_collection import ServiceCollection
from framework.di.static_provider import ProviderBase
from httpx import AsyncClient
from motor.motor_asyncio import AsyncIOMotorClient

from clients.email_gateway_client import EmailGatewayClient
from clients.event_client import EventClient
from clients.identity_client import IdentityClient
from clients.kasa_client import KasaClient
from clients.nest_client import NestClient
from data.nest_history_repository import NestThermostatHistoryRepository
from data.nest_integration_repository import NestIntegrationRepository
from data.nest_sensor_repository import NestDeviceRepository, NestSensorRepository
from domain.auth import AuthPolicy
from services.alert_service import AlertService
from services.command_service import NestCommandService
from services.device_service import NestDeviceService
from services.event_service import EventService
from services.integration_service import NestIntegrationService
from services.nest_service import NestService


def configure_azure_ad(container):
    configuration = container.resolve(Configuration)

    # Hook the Azure AD auth config into the service
    # configuration
    ad_auth: AzureAdConfiguration = configuration.ad_auth
    azure_ad = AzureAd(
        tenant=ad_auth.tenant_id,
        audiences=ad_auth.audiences,
        issuer=ad_auth.issuer)

    azure_ad.add_authorization_policy(
        name=AuthPolicy.Default,
        func=lambda t: True)

    azure_ad.add_authorization_policy(
        name=AuthPolicy.Read,
        func=lambda t: 'Nest.Read' in t.get('roles', []))

    # TODO: Remove default auth policy

    return azure_ad


def configure_http_client(container):
    return AsyncClient(timeout=None)


def configure_mongo_client(container):
    configuration = container.resolve(Configuration)

    connection_string = configuration.mongo.get('connection_string')
    client = AsyncIOMotorClient(connection_string)

    return client


def register_clients(descriptors: ServiceCollection):
    descriptors.add_singleton(
        dependency_type=AsyncClient,
        factory=configure_http_client)

    descriptors.add_singleton(CacheClientAsync)
    descriptors.add_singleton(FeatureClientAsync)
    descriptors.add_singleton(IdentityClient)
    descriptors.add_singleton(EventClient)
    descriptors.add_singleton(NestClient)
    descriptors.add_singleton(EmailGatewayClient)
    descriptors.add_singleton(KasaClient)


def register_repositories(descriptors: ServiceCollection):
    descriptors.add_singleton(NestDeviceRepository)
    descriptors.add_singleton(NestSensorRepository)
    descriptors.add_singleton(NestIntegrationRepository)
    descriptors.add_singleton(NestThermostatHistoryRepository)


def register_services(descriptors: ServiceCollection):
    descriptors.add_singleton(NestService)
    descriptors.add_singleton(NestCommandService)
    descriptors.add_singleton(NestIntegrationService)
    descriptors.add_singleton(NestDeviceService)
    descriptors.add_singleton(EventService)
    descriptors.add_singleton(AlertService)


def register_providers(descriptors: ServiceCollection):
    pass


class ContainerProvider(ProviderBase):
    @classmethod
    def configure_container(cls):
        container = ServiceCollection()

        container.add_singleton(Configuration)
        container.add_singleton(
            dependency_type=AsyncIOMotorClient,
            factory=configure_mongo_client)
        container.add_singleton(
            dependency_type=AzureAd,
            factory=configure_azure_ad)

        register_clients(container)
        register_repositories(container)
        register_services(container)
        register_providers(container)

        return container
