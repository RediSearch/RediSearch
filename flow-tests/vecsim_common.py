"""
Common utilities for vecsim disk flow tests.

This module provides shared helper functions for testing disk-based
vector indexes through Redis.
"""

import struct


def create_float32_vector(values: list[float]) -> bytes:
    """Create a FLOAT32 vector as bytes.

    Args:
        values: List of float values for the vector.

    Returns:
        Packed bytes in FLOAT32 format suitable for Redis vector fields.
    """
    return struct.pack(f'{len(values)}f', *values)


def _parse_vecsim_list(info_list) -> dict:
    """Parse a flat key-value list into a dictionary."""
    result = {}
    for i in range(0, len(info_list), 2):
        key = info_list[i].decode() if isinstance(info_list[i], bytes) else info_list[i]
        value = info_list[i + 1]
        if isinstance(value, bytes):
            value = value.decode()
        elif isinstance(value, list):
            value = _parse_vecsim_list(value)
        result[key] = value
    return result


def get_vecsim_info(conn, index_name: str, field_name: str) -> dict:
    """Get vector index info as a dictionary.

    Args:
        conn: Redis connection.
        index_name: Name of the FT index.
        field_name: Name of the vector field.

    Returns:
        Dictionary with vector index info. For tiered indexes, nested structures
        like FRONTEND_INDEX and BACKEND_INDEX are also converted to dicts.
    """
    info = conn.execute_command('_FT.DEBUG', 'VECSIM_INFO', index_name, field_name)
    return _parse_vecsim_list(info)
