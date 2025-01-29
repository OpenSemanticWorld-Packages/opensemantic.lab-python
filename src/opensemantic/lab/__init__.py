# Necessary to allow a submodule to be defined in a separate repository and use this
#  package as its namespace package:
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from importlib.metadata import PackageNotFoundError, version  # pragma: no cover

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = "opensemantic.lab"
    __version__ = version(dist_name)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
finally:
    del version, PackageNotFoundError
