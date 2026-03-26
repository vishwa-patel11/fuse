"""
Dynamic loader for client-specific modules.
Enables CI/CD: building for client X = common + clients/X only.
"""
import importlib
import logging
import os
import sys

# Ensure clients package is importable (workspace root = parent of common)
_common_dir = os.path.dirname(os.path.abspath(__file__))
_workspace_root = os.path.dirname(_common_dir)
if _workspace_root not in sys.path:
    sys.path.insert(0, _workspace_root)

logger = logging.getLogger(__name__)


def _import_client_module(client, module_name):
    """Import clients.{client}.{module_name}, return module or None on ImportError."""
    # Try client as-is, then case variants (handles njh/NJH, wfp/WFP etc.)
    seen = set()
    for name in [client, client.upper(), client.lower()]:
        if name in seen:
            continue
        seen.add(name)
        try:
            mod = importlib.import_module(f"clients.{name}.{module_name}")
            return mod
        except ImportError as e:
            logger.debug("clients_loader: %s.%s not found: %s", name, module_name, e)
            continue
    return None


def get_client_parser(client):
    """
    Get the client's data.parquet transform function.
    Returns a callable(df, client) or None if not found.
    """
    mod = _import_client_module(client, "parser")
    if mod is None:
        return None
    fn = getattr(mod, "parse", None)
    if fn is None:
        fn = getattr(mod, client, None)
    return fn if callable(fn) else None


def get_client_fn(client, fn_suffix, module_name="source_code"):
    """
    Get a client-specific function by suffix.
    E.g. get_client_fn('AFHU', 'initiate_codes') -> AFHU_initiate_codes from clients.AFHU.source_code
    Returns callable or None.
    """
    mod = _import_client_module(client, module_name)
    if mod is None:
        return None
    fn_name = f"{client}_{fn_suffix}"
    fn = getattr(mod, fn_suffix, None) or getattr(mod, fn_name, None)
    return fn if callable(fn) else None


def get_client_base_table_fn(client, fn_suffix):
    """
    Get a client-specific base table function by suffix.
    E.g. get_client_base_table_fn('FFB', 'source_code_transform') -> FFB_source_code_transform
    Returns callable or None.
    """
    return get_client_fn(client, fn_suffix, module_name="base_table")
