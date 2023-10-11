

class AuthClientNotFoundException(Exception):
    def __init__(
        self,
        client_name: str,
        *args: object
    ) -> None:
        super().__init__(
            f"No registered client with the name '{client_name}' could be found"
        )


class AuthTokenFailureException(Exception):
    def __init__(
        self,
        client_name: str,
        status_code: int,
        message: str,
        *args: object
    ) -> None:
        super().__init__(
            f"Failed to fetch auth token for client '{client_name}' with status '{status_code}': {message}"
        )


class NestThermostatTemperatureException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class NestThermostatUnknownCommandException(Exception):
    def __init__(self, command_type, *args: object) -> None:
        super().__init__(f"Nest command type '{command_type}' is not known")


class NestAuthorizationFailureException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__('Failed to fetch Nest token from auth client')
