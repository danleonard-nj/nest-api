from typing import Dict, List
from framework.serialization import Serializable


class AuthPolicy:
    Default = 'default'


class AuthKey:
    NestApiKey = 'nest-sensor-api-key'


class ClientScope:
    EmailGatewayApi = 'api://4ff83655-c28e-478f-b384-08ca8e98a811/.default'
    TwilioGatewayApi = 'api://608043f8-87a6-46bd-ab49-1b73de73a6ec/.default'
    AzureGatewayApi = 'api://a6d4c26f-f77c-41dc-b732-eb82ac0fbe39/.default'


class AuthClientConfig(Serializable):
    DefaultGrantType = 'client_credentials'

    def __init__(
        self,
        data: Dict
    ):
        self.client_id = data.get('client_id')
        self.client_secret = data.get('client_secret')

        self.grant_type = data.get(
            'grant_type', self.DefaultGrantType)

        self.scopes = self.__parse_scopes(
            scopes=data.get('scopes', list()))

    def __parse_scopes(
        self,
        scopes: List[str]
    ) -> str:

        return ' '.join(scopes)
