"""Bounded authenticated client for the worker-management channel."""

from __future__ import annotations

import hmac
import os
import socket
from multiprocessing import connection as mp_connection
from multiprocessing.connection import Connection
from multiprocessing.context import AuthenticationError
from typing import Any

from wetlands.protocol import EXECUTION_PROTOCOL_VERSION, MANAGEMENT_PROTOCOL_VERSION, WORKER_RUNTIME_VERSION


class ManagementConnectionError(RuntimeError):
    """A worker management endpoint could not be trusted or reached."""


def _mp_connection_attr(*names: str) -> Any:
    for name in names:
        if hasattr(mp_connection, name):
            return getattr(mp_connection, name)
    raise RuntimeError(f"multiprocessing.connection does not expose any of {names!r}")


def _recv_bytes(connection: Connection, timeout: float, maxlength: int) -> bytes:
    if not mp_connection.wait([connection], timeout):
        raise TimeoutError("Timed out waiting for worker management authentication")
    return connection.recv_bytes(maxlength)


def _answer_challenge(connection: Connection, authkey: bytes, timeout: float) -> None:
    challenge = _mp_connection_attr("_CHALLENGE", "CHALLENGE")
    welcome = _mp_connection_attr("_WELCOME", "WELCOME")
    message = _recv_bytes(connection, timeout, 256)
    if not message.startswith(challenge):
        raise AuthenticationError("Worker management endpoint sent an invalid challenge")
    message = message[len(challenge) :]
    minimum = getattr(mp_connection, "_MD5ONLY_MESSAGE_LENGTH", None)
    if minimum is not None and len(message) < minimum:
        raise AuthenticationError("Worker management endpoint sent a short challenge")
    if hasattr(mp_connection, "_create_response"):
        digest = getattr(mp_connection, "_create_response")(authkey, message)
    else:
        digest = hmac.new(authkey, message, "md5").digest()
    connection.send_bytes(digest)
    if _recv_bytes(connection, timeout, 256) != welcome:
        raise AuthenticationError("Worker management authentication was rejected")


def _deliver_challenge(connection: Connection, authkey: bytes, timeout: float) -> None:
    challenge = _mp_connection_attr("_CHALLENGE", "CHALLENGE")
    welcome = _mp_connection_attr("_WELCOME", "WELCOME")
    failure = _mp_connection_attr("_FAILURE", "FAILURE")
    message_length = _mp_connection_attr("MESSAGE_LENGTH")
    message = os.urandom(message_length)
    if hasattr(mp_connection, "_verify_challenge"):
        message = b"{sha256}" + message
    connection.send_bytes(challenge + message)
    response = _recv_bytes(connection, timeout, 256)
    if hasattr(mp_connection, "_verify_challenge"):
        try:
            getattr(mp_connection, "_verify_challenge")(authkey, message, response)
        except AuthenticationError:
            connection.send_bytes(failure)
            raise
    elif response != hmac.new(authkey, message, "md5").digest():
        connection.send_bytes(failure)
        raise AuthenticationError("Worker management endpoint returned the wrong digest")
    connection.send_bytes(welcome)


def _connect(port: int, authkey: bytes, timeout: float) -> Connection:
    sock = socket.socket(socket.AF_INET)
    try:
        sock.settimeout(timeout)
        sock.connect(("127.0.0.1", port))
        sock.setblocking(True)
        connection = Connection(sock.detach())
    except Exception:
        sock.close()
        raise
    try:
        _answer_challenge(connection, authkey, timeout)
        _deliver_challenge(connection, authkey, timeout)
    except Exception:
        connection.close()
        raise
    return connection


def _identity(entry: dict[str, Any]) -> dict[str, object]:
    return {
        "worker_id": entry["worker_id"],
        "pid": entry["pid"],
        "environment_path": entry["env_path"],
        "generation_id": entry["generation_id"],
        "recipe_hash": entry["recipe_hash"],
        "worker_runtime_version": WORKER_RUNTIME_VERSION,
        "execution_protocol_version": EXECUTION_PROTOCOL_VERSION,
        "pool_id": entry.get("pool_id"),
        "worker_index": entry["worker_index"],
    }


def _validate_message(message: object, expected: dict[str, object], action: str) -> dict[str, Any]:
    if not isinstance(message, dict):
        raise ManagementConnectionError("Worker management response was not an object")
    if message.get("action") != action:
        raise ManagementConnectionError(f"Worker management response had unexpected action {message.get('action')!r}")
    if message.get("management_protocol_version") != MANAGEMENT_PROTOCOL_VERSION:
        raise ManagementConnectionError("Worker management protocol version is incompatible")
    mismatches = {
        field: (value, message.get(field)) for field, value in expected.items() if message.get(field) != value
    }
    if mismatches:
        details = ", ".join(
            f"{field}: expected {expected_value!r}, got {actual!r}"
            for field, (expected_value, actual) in sorted(mismatches.items())
        )
        raise ManagementConnectionError(f"Worker management identity mismatch ({details})")
    return message


def start_debugger(
    entry: dict[str, Any],
    authkey: bytes,
    *,
    timeout: float = 5.0,
    startup_timeout: float = 30.0,
) -> dict[str, Any]:
    """Start debugpy in one exact worker and return its validated response."""
    expected = _identity(entry)
    try:
        connection = _connect(int(entry["management_port"]), authkey, timeout)
    except (EOFError, OSError, TimeoutError, AuthenticationError) as error:
        raise ManagementConnectionError(f"Could not contact worker {entry['worker_id']!r}: {error}") from error
    try:
        if not mp_connection.wait([connection], timeout):
            raise TimeoutError("Timed out waiting for worker management identity")
        hello = _validate_message(connection.recv(), expected, "management_hello")
        connection.send(
            {
                "action": "start_debugger",
                "management_protocol_version": MANAGEMENT_PROTOCOL_VERSION,
                "worker_id": hello["worker_id"],
            }
        )
        if not mp_connection.wait([connection], startup_timeout):
            raise TimeoutError("Timed out waiting for debugger startup")
        response = connection.recv()
        if isinstance(response, dict) and response.get("action") == "error":
            _validate_message(response, expected, "error")
            raise ManagementConnectionError(str(response.get("message") or "Worker could not start its debugger"))
        return _validate_message(response, expected, "debugger_started")
    except (EOFError, OSError, TimeoutError, AuthenticationError) as error:
        raise ManagementConnectionError(f"Could not contact worker {entry['worker_id']!r}: {error}") from error
    finally:
        connection.close()
