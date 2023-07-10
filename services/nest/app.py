from dotenv import load_dotenv
from framework.abstractions.abstract_request import RequestContextProvider
from framework.di.static_provider import InternalProvider
from framework.serialization.serializer import configure_serializer
from quart import Quart

from routes.nest import nest_bp
from routes.command import command_bp
from routes.sensor import sensor_bp
from routes.health import health_bp
from utils.provider import ContainerProvider


load_dotenv()

app = Quart(__name__)

ContainerProvider.initialize_provider()
InternalProvider.bind(ContainerProvider.get_service_provider())


app.register_blueprint(health_bp)
app.register_blueprint(nest_bp)
app.register_blueprint(command_bp)
app.register_blueprint(sensor_bp)


@app.before_serving
async def startup():
    RequestContextProvider.initialize_provider(
        app=app)

configure_serializer(app)


if __name__ == '__main__':
    app.run(debug=True, port='5091')
