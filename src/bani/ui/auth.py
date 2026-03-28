"""Authentication middleware for the Bani Web UI (Section 20.4).

All API endpoints require a Bearer token that matches the server's
randomly-generated auth token. The token is printed to the console
when the server starts, and clients must include it in the
``Authorization: Bearer <token>`` header.
"""

from __future__ import annotations

from fastapi import Header, HTTPException, Request


def verify_token(
    request: Request,
    authorization: str = Header(..., alias="Authorization"),
) -> str:
    """Validate the Bearer token against the server's auth token.

    The expected token is stored in ``request.app.state.auth_token``
    by :class:`BaniUIServer` at startup.

    Args:
        request: The incoming request (injected by FastAPI).
        authorization: The ``Authorization`` header value.

    Returns:
        The validated token string.

    Raises:
        HTTPException: 401 if the token is missing, malformed, or invalid.
    """
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    token = authorization[len("Bearer ") :]
    expected: str = request.app.state.auth_token

    if token != expected:
        raise HTTPException(status_code=401, detail="Invalid token")

    return token
