"""Dynamic sliding-window context compression for agent histories."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from app.config import settings


class ContextManager:
    """Optimize transient LLM payloads without mutating persisted history."""

    async def optimize_history(self, history: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return a compressed copy of history when it exceeds the token threshold."""

        if not history:
            return []

        history_copy = deepcopy(history)
        if self._estimate_tokens(history_copy) <= settings.CONTEXT_TOKEN_THRESHOLD:
            return history_copy

        if len(history_copy) <= 3:
            return history_copy

        middle_end = len(history_copy) - 2
        for index in range(1, middle_end):
            frame = history_copy[index]
            if self._is_verbose_tool_response(frame):
                frame["content"] = self._summarize_tool_response(frame)
                frame["compressed"] = True
                frame["compression_strategy"] = "sliding_window_tool_response_summary"

        return history_copy

    def _estimate_tokens(self, history: list[dict[str, Any]]) -> int:
        footprint = 0
        for frame in history:
            footprint += max(1, len(str(frame)) // 4)
        return footprint

    def _is_verbose_tool_response(self, frame: dict[str, Any]) -> bool:
        step_type = str(frame.get("step_type", frame.get("type", ""))).upper()
        role = str(frame.get("role", "")).lower()
        content = frame.get("content", frame.get("output_payload", ""))
        return (step_type == "TOOL_RESPONSE" or role == "tool") and len(str(content)) > 512

    def _summarize_tool_response(self, frame: dict[str, Any]) -> str:
        content = frame.get("content", frame.get("output_payload", ""))
        text = str(content).replace("\n", " ").strip()
        head = text[:700]
        tail = text[-300:] if len(text) > 1000 else ""
        omitted = max(0, len(text) - len(head) - len(tail))
        if tail:
            return (
                "Compressed TOOL_RESPONSE summary: "
                f"{head} ... [omitted {omitted} characters] ... {tail}"
            )
        return f"Compressed TOOL_RESPONSE summary: {head}"
