import enum


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


class IntergationDeviceType(enum.StrEnum):
    Plug = 'plug'
    Fan = 'fan'


class IntegrationEventType(enum.StrEnum):
    PowerCycle = 'power-cycle'


class IntegrationEventResult(enum.StrEnum):
    Success = 'success'
    Failure = 'failure'
    NotSupported = 'not-supported'
    MinimumIntervalNotMet = 'minimum-interval-not-met'
    NoOp = 'no-op'
    InvalidConfiguration = 'invalid-configuration'


class KasaIntegrationSceneType(enum.StrEnum):
    PowerOn = 'on'
    PowerOff = 'off'
