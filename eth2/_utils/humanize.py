# The number of characters to display from the prefix and suffix in the resulting abbreviation
DISPLAY_CHARS = 4
# The number of bytes that result in an abbreviation of the
# same length; consequently, just inline the value
INLINE_LENGTH = 6


def humanize_bytes(value: bytes) -> str:
    """
    Cribbed from ``eth_utils`` implementation of ``humanize_hash``.

    This function accepts a ``bytes`` value of any length, rather than only a ``Hash32``.
    """
    if len(value) < INLINE_LENGTH:
        payload = value.hex()
    else:
        hex = value.hex()
        head = hex[:DISPLAY_CHARS]
        tail = hex[-1 * DISPLAY_CHARS :]
        payload = f"{head}..{tail}"

    return f"bytes{len(value)}({payload})"
