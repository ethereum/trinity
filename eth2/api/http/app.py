from quart_trio import QuartTrio

from .converters import HexadecimalPublicKeyConverter


app = QuartTrio(__name__)
app.url_map.converters['pubkey'] = HexadecimalPublicKeyConverter
