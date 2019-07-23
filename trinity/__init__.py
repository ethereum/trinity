import pkg_resources

# TODO: update this to use the `trinity` version once extracted from py-evm
__version__: str
try:
    __version__ = pkg_resources.get_distribution("trinity").version
except pkg_resources.DistributionNotFound:
    __version__ = f"eth-{pkg_resources.get_distribution('py-evm').version}"

# This is to ensure we call setup_extended_logging() before anything else.
import eth as _eth_module  # noqa: F401


from .main import (  # noqa: F401
    main,
)

from .main_beacon import (  # noqa: F401
    main_beacon,
)
