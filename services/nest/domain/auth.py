from typing import Dict, List
from framework.serialization import Serializable


class AuthPolicy:
    Default = 'default'
    Read = 'read'


class AuthKey:
    NestApiKey = 'nest-sensor-api-key'


class ClientScope:
    EmailGatewayApi = 'api://4ff83655-c28e-478f-b384-08ca8e98a811/.default'
    KasaApi = 'api://f1c68acc-5b7d-4958-9eff-a777d8e67979/.default'


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
