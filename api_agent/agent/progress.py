"""Minimal turn tracking for REST and GraphQL agents."""

from contextvars import ContextVar

_turn: ContextVar[list[int]] = ContextVar("turn")  # [turn_number]


def reset_progress():
    """Reset at start of each request."""
    _turn.set([0])


def increment_turn():
    """Increment turn counter."""
    _turn.get()[0] += 1


def get_turn_context(max_turns: int) -> str:
    """Return turn count string for injection."""
    turn = _turn.get()[0]
    return f"Turn {turn}/{max_turns}"
