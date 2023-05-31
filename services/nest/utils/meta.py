from functools import wraps
from typing import Callable, List

from quart import Blueprint

from framework.auth.wrappers.azure_ad_wrappers import azure_ad_authorization
from framework.auth.wrappers.key_authorization import key_authorization
from framework.di.static_provider import inject_container_async
from framework.exceptions.nulls import ArgumentNullException
from framework.handlers.response_handler_async import response_handler


class MetaBlueprint(Blueprint):
    def __get_endpoint(self, view_function: Callable):
        return f'__route__{view_function.__name__}'

    def configure(self,  rule: str, methods: List[str], auth_scheme: str):
        def decorator(function):
            @self.route(rule, methods=methods, endpoint=self.__get_endpoint(function))
            @response_handler
            @azure_ad_authorization(scheme=auth_scheme)
            @inject_container_async
            @wraps(function)
            async def wrapper(*args, **kwargs):
                return await function(*args, **kwargs)
            return wrapper
        return decorator

    def with_key_auth(
        self,
        rule: str,
        methods: List[str],
        key_name: str
    ):
        '''
        Register a route with API key authorization
        '''

        ArgumentNullException.if_none_or_whitespace(rule, 'rule')
        ArgumentNullException.if_none_or_whitespace(key_name, 'key_name')
        ArgumentNullException.if_none_or_empty(methods, 'methods')

        def decorator(function):
            @self.route(rule, methods=methods, endpoint=self.__get_endpoint(function))
            @response_handler
            @key_authorization(name=key_name)
            @inject_container_async
            @wraps(function)
            async def wrapper(*args, **kwargs):
                return await function(*args, **kwargs)
            return wrapper
        return decorator


class OpenAuthBlueprint(Blueprint):
    def __get_endpoint(self, view_function: Callable):
        return f'__route__{view_function.__name__}'

    def configure(self,  rule: str, methods: List[str]):
        def decorator(function):
            @self.route(rule, methods=methods, endpoint=self.__get_endpoint(function))
            @response_handler
            @inject_container_async
            @wraps(function)
            async def wrapper(*args, **kwargs):
                return await function(*args, **kwargs)
            return wrapper
        return decorator
