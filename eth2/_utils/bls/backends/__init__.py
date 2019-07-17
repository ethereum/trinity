
from .noop import NoOpBackend
from .py_ecc import PyECCBackend


AVAILABLE_BACKENDS = (
    NoOpBackend,
    PyECCBackend,
)


# If blspy not installed, use PyECC as default BLS backend

try:
    from .chia import ChiaBackend
    DEFAULT_BACKEND = ChiaBackend
    AVAILABLE_BACKENDS += ChiaBackend
except ImportError:
    DEFAULT_BACKEND = PyECCBackend
