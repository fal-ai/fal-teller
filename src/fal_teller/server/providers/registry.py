from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Type, Union

import importlib_metadata

if TYPE_CHECKING:
    from fal_teller.server.providers.base import ArrowProvider

# Any new providers can register themselves during package installation
# time by simply adding an entry point to the `fal_teller.server.providers` group.
_ENTRY_POINT = "fal_teller.server.providers"


_PROVIDER_REGISTRY: Dict[
    str, Union[importlib_metadata.EntryPoint, Type["ArrowProvider"]]
] = {}


def _reload_registry() -> Dict[str, Type[ArrowProvider]]:
    entry_points = importlib_metadata.entry_points()
    _PROVIDER_REGISTRY.update(
        {
            # We are not immediately loading the provider class here
            # since it might cause importing modules that we won't be
            # using at all.
            entry_point.name: entry_point
            for entry_point in entry_points.select(group=_ENTRY_POINT)
        }
    )


def _get_from_registry(key: str) -> Type["ArrowProvider"]:
    """Either return a cached ArrowProvider class or load it dynamically
    from the registered entry point, cache it, and return ti."""
    provider_or_ep = _PROVIDER_REGISTRY[key]
    if isinstance(provider_or_ep, importlib_metadata.EntryPoint):
        _PROVIDER_REGISTRY[key] = provider_or_ep = provider_or_ep.load()
    return provider_or_ep


_reload_registry()


def get_provider(
    kind: str,
    *args: Any,
    **kwargs: Any,
) -> ArrowProvider:
    """Get the provider of the given kind with the supplied
    configuration."""

    try:
        registered_provider_cls = _get_from_registry(kind)
    except KeyError as exc:
        raise ValueError(f"Unknown data provider: '{kind}'") from exc
    else:
        return registered_provider_cls(*args, **kwargs)
