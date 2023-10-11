import asyncio

from framework.clients.cache_client import CacheClientAsync
from framework.configuration import Configuration
from framework.logger import get_logger
from framework.validators.nulls import none_or_whitespace

from clients.nest_client import NestClient
from domain.cache import CacheKey
from domain.enums import NestCommandType, ThermostatMode
from domain.exceptions import (NestThermostatTemperatureException,
                               NestThermostatUnknownCommandException)
from domain.nest import CommandListItem, NestCommandTypeMapping, NestThermostat
from domain.rest import (NestCommandClientRequest, NestCommandHandlerResponse,
                         NestCommandRequest)
from utils.utils import to_celsius

logger = get_logger(__name__)


class NestCommandService:
    def __init__(
        self,
        configuration: Configuration,
        nest_client: NestClient,
        cache_client: CacheClientAsync
    ):
        self.__thermostat_id = configuration.nest.get(
            'thermostat_id')

        self.__minimum_allowed_temperature = configuration.nest.get(
            'minimum_allowed_temperature')
        self.__maximum_allowed_temperature = configuration.nest.get(
            'maximum_allowed_temperature')

        self.__nest_client = nest_client
        self.__cache_client = cache_client

    async def __bust_thermostat_mode_cache(
        self
    ):
        key = CacheKey.active_thermostat_mode()

        logger.info(f'Busting thermostat mode cache: {key}')
        await self.__cache_client.delete_key(
            key=key)

    async def __get_active_thermostat_mode(
        self
    ):
        logger.info('Get thermostat mode')

        key = CacheKey.active_thermostat_mode()

        cached_mode = await self.__cache_client.get_cache(
            key=key)

        if not none_or_whitespace(cached_mode):
            parsed_mode = ThermostatMode(cached_mode)
            logger.info(f'Returning cached thermostat mode: {parsed_mode}')

            return parsed_mode

        data = await self.__nest_client.get_thermostat()

        thermostat = NestThermostat.from_json_object(
            data=data,
            thermostat_id=self.__thermostat_id)

        mode = thermostat.thermostat_mode

        # Cache the thermostat mode async
        asyncio.create_task(
            self.__cache_client.set_cache(
                key=CacheKey.active_thermostat_mode(),
                value=mode,
                ttl=60 * 24))

        return mode

    async def handle_command(
        self,
        command_request: NestCommandRequest
    ):
        status = await self.__delegate_command(
            command_type=command_request.command_type,
            params=command_request.params)

        return NestCommandHandlerResponse(
            command_type=command_request.command_type,
            params=command_request.params,
            status=status)

    async def set_thermostat_mode(
        self,
        mode: ThermostatMode,
        delay_seconds: int = 1
    ):
        current_mode = await self.__get_active_thermostat_mode()

        if current_mode == mode:
            logger.info(f'Thermostat mode is already set to {mode}')
            return

        asyncio.create_task(
            self.__bust_thermostat_mode_cache())

        command = NestCommandClientRequest(
            command=NestCommandTypeMapping[NestCommandType.SetPowerOff],
            mode=mode.value
        )

        logger.info(f'Set mode: {mode}: {command.to_dict()}')

        result = await self.__nest_client.execute_command(
            command=command.to_dict())

        logger.info(f'Result: {result}')

        # Optional delay after the mode is set to allow the thermostat
        # to update
        if delay_seconds > 0:
            logger.info(f'Sleeping for {delay_seconds} seconds')
            await asyncio.sleep(delay_seconds)

        return result

    async def set_heat(
        self,
        params: dict
    ) -> dict:

        logger.info(f'Set heat: {params}')
        heat_degrees_fahrenheit = params.get('heat_degrees_fahrenheit')

        if heat_degrees_fahrenheit > self.__maximum_allowed_temperature:
            raise NestThermostatTemperatureException(
                f'Heat degrees: {heat_degrees_fahrenheit}: exceeds maximum temp: {self.__maximum_allowed_temperature}')

        # Set the thermostat mode to heat
        logger.info('Setting thermostat mode to heat')
        await self.set_thermostat_mode(
            mode=ThermostatMode.Heat)

        # Generate the command
        command = NestCommandClientRequest(
            command=NestCommandTypeMapping[NestCommandType.SetHeat],
            heatCelsius=to_celsius(heat_degrees_fahrenheit)
        )

        logger.info(f'Command: {command.to_dict()}')
        return await self.__nest_client.execute_command(
            command=command.to_dict())

    async def set_cool(
        self,
        params: dict
    ) -> dict:

        logger.info(f'Set cool: {params}')
        cool_degrees_fahrenheit = params.get(
            'cool_degrees_fahrenheit')

        if cool_degrees_fahrenheit < self.__minimum_allowed_temperature:
            logger.info(
                f'Cool degrees: {cool_degrees_fahrenheit}: exceeds minimum temp: {self.__minimum_allowed_temperature}')

            raise Exception('Too cold!')

        # Set the thermostat mode to cool
        logger.info('Setting thermostat mode to cool')
        await self.set_thermostat_mode(
            mode=ThermostatMode.Cool)

        # Generate the command
        command = NestCommandClientRequest(
            command=NestCommandTypeMapping[NestCommandType.SetCool],
            coolCelsius=to_celsius(cool_degrees_fahrenheit)
        )

        logger.info(f'Command: {command.to_dict()}')
        return await self.__nest_client.execute_command(
            command=command.to_dict())

    async def set_range(
        self,
        params: dict
    ) -> dict:

        heat_degrees_fahrenheit = params.get('heat_degrees_fahrenheit')
        cool_degrees_fahrenheit = params.get('cool_degrees_fahrenheit')

        if heat_degrees_fahrenheit > self.__maximum_allowed_temperature:
            raise NestThermostatTemperatureException(
                f'Temperature exceeds safety maximum of {self.__maximum_allowed_temperature} degrees fahrenheit')

        if cool_degrees_fahrenheit < self.__minimum_allowed_temperature:
            raise NestThermostatTemperatureException(
                f'Temperature falls below safety minimum of {self.__minimum_allowed_temperature} degrees fahrenheit')

        # Set the thermostat mode to range
        await self.set_thermostat_mode(
            mode=ThermostatMode.Range)

        # Generate the command
        command = NestCommandClientRequest(
            command=NestCommandTypeMapping[NestCommandType.SetRange],
            heatCelsius=to_celsius(heat_degrees_fahrenheit),
            coolCelsius=to_celsius(cool_degrees_fahrenheit)
        )

        logger.info(f'Set range: {command.to_dict()}')
        return await self.__nest_client.execute_command(
            command=command.to_dict())

    async def set_power_off(
        self
    ):
        logger.info('Set power off')

        return await self.set_thermostat_mode(
            mode=ThermostatMode.Off,
            delay_seconds=0)

    async def list_commands(
        self
    ) -> list[CommandListItem]:
        logger.info(f'Listing commands')

        commands = []
        for command in NestCommandType:

            value = CommandListItem(
                command=command.name,
                key=command.value)

            commands.append(value)

        return commands

    async def __delegate_command(
        self,
        command_type: NestCommandType,
        params: dict
    ) -> dict:

        logger.info(f'Delegate command type: {command_type}: {params}')

        match command_type:
            case NestCommandType.SetPowerOff:
                return await self.set_power_off()
            case NestCommandType.SetCool:
                return await self.set_cool(params)
            case NestCommandType.SetHeat:
                return await self.set_heat(params)
            case NestCommandType.SetRange:
                return await self.set_range(params)
            case NestCommandType.SetPowerOff:
                return await self.set_thermostat_mode(params)
            case _:
                raise NestThermostatUnknownCommandException(
                    command_type=command_type)
