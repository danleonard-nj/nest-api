import enum

NEST_COMMAND_SET_HEAT = 'sdm.devices.commands.ThermostatTemperatureSetpoint.SetHeat'
NEST_COMMAND_SET_COOL = 'sdm.devices.commands.ThermostatTemperatureSetpoint.SetCool'
NEST_COMMAND_SET_RANGE = 'sdm.devices.commands.ThermostatTemperatureSetpoint.SetRange'
NEST_COMMAND_SET_MODE = 'sdm.devices.commands.ThermostatMode.SetMode'


class HealthStatus(enum.StrEnum):
    Healthy = 'healthy'
    Unhealthy = 'unhealthy'


class ThermostatMode(enum.StrEnum):
    Heat = 'HEAT'
    Cool = 'COOL'
    Range = 'HEATCOOL'
    Off = 'OFF'


class NestCommandType(enum.StrEnum):
    SetRange = 'set-range'
    SetHeat = 'set-heat'
    SetCool = 'set-cool'
    SetPowerOff = 'set-power-off'
    SetMode = 'set-mode'


class IntergationDeviceType(enum.StrEnum):
    Plug = 'plug'
    Fan = 'fan'


class IntegrationEventType(enum.StrEnum):
    PowerCycle = 'power-cycle'


class IntegrationEventResult(enum.StrEnum):
    Success = 'success'
    Failure = 'failure'
    NotSupported = 'not-supported'
    MinimumInterval = 'minimum-interval'
    NoAction = 'no-action'
    InvalidConfiguration = 'invalid-configuration'
    Error = 'error'


class KasaIntegrationSceneType(enum.StrEnum):
    PowerOn = 'device_on'
    PowerOff = 'device_off'


class Feature(enum.StrEnum):
    NestHealthCheckEmailAlerts = 'nest-health-check-email-alerts'


class NestCommand(enum.StrEnum):
    SetMode = NEST_COMMAND_SET_MODE
    SetCool = NEST_COMMAND_SET_COOL
    SetHeat = NEST_COMMAND_SET_HEAT
    SetRange = NEST_COMMAND_SET_RANGE
