from quart_trio import QuartTrio

from .converters import HexadecimalPublicKeyConverter
from .views import blueprint


app = QuartTrio(__name__)
app.url_map.converters['pubkey'] = HexadecimalPublicKeyConverter

app.register_blueprint(blueprint)
