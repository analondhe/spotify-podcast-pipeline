"""Model providers: the only part of the agent that knows which LLM API it talks to.

Each provider owns its native message history and exposes the same small surface:
add a user message, stream one assistant turn, add tool results. The agent loop in
``chat.py`` works purely in terms of ``Turn`` / ``ToolCall`` / ``ToolResult``.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Iterator

PROVIDERS = ("anthropic", "ollama")
DEFAULT_MODELS = {"anthropic": "claude-opus-5-5", "ollama": "qwen3:8b"}


@dataclass
class ToolCall:
    id: str
    name: str
    args: object  # usually a dict; validated by the executor


@dataclass
class ToolResult:
    id: str
    name: str
    content: str
    is_error: bool = False


@dataclass
class Turn:
    text: str
    tool_calls: list[ToolCall] = field(default_factory=list)
    stop_reason: str = "end_turn"  # end_turn | tool_use | refusal | max_tokens
    model: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0


class ProviderError(RuntimeError):
    """The model API failed; the caller should roll back the pending turn."""


class Provider:
    name = "base"
    extra_system = ""  # provider-specific guidance appended to the system prompt

    def __init__(self, system: str, tools: list[dict], model: str):
        self.system = system + ("\n" + self.extra_system if self.extra_system else "")
        self.tools = tools
        self.model = model
        self.messages: list[dict] = []

    # history bookkeeping shared by all providers
    def reset(self) -> None:
        self.messages = []

    def mark(self) -> int:
        return len(self.messages)

    def rollback(self, mark: int) -> None:
        del self.messages[mark:]

    def add_user(self, text: str) -> None:
        self.messages.append({"role": "user", "content": text})

    def add_tool_results(self, results: list[ToolResult]) -> None:
        raise NotImplementedError

    def stream(self) -> Iterator[str | Turn]:
        """Yield text deltas, then exactly one Turn. Appends the assistant turn to history."""
        raise NotImplementedError


# -- Anthropic ----------------------------------------------------------------

class AnthropicProvider(Provider):
    name = "anthropic"
    MAX_TOKENS = 16000

    def __init__(self, system: str, tools: list[dict], model: str, client=None,
                 effort: str = "medium", **_ignored):
        super().__init__(system, tools, model)
        import anthropic

        self._anthropic = anthropic
        self.client = client or anthropic.Anthropic()
        self.effort = effort

    def add_tool_results(self, results: list[ToolResult]) -> None:
        self.messages.append({
            "role": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": r.id, "content": r.content, "is_error": r.is_error}
                for r in results
            ],
        })

    def _request(self):
        return self.client.beta.messages.stream(
            model=self.model,
            max_tokens=self.MAX_TOKENS,
            system=[{"type": "text", "text": self.system, "cache_control": {"type": "ephemeral"}}],
            tools=self.tools,
            messages=self.messages,
            output_config={"effort": self.effort},
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
        )

    def stream(self) -> Iterator[str | Turn]:
        json_retries = 0
        while True:
            try:
                with self._request() as stream:
                    for event in stream:
                        if event.type == "text":
                            yield event.text
                    response = stream.get_final_message()
            except ValueError:
                # Tool input the SDK could not parse at all; re-issue the turn (bounded).
                json_retries += 1
                if json_retries > 2:
                    raise ProviderError("the model produced unreadable tool input three times")
                continue
            except self._anthropic.APIError as exc:
                raise ProviderError(f"API error: {exc}") from exc

            if response.stop_reason == "pause_turn":
                self.messages.append({"role": "assistant", "content": response.content})
                continue
            break

        usage = response.usage
        text = "".join(b.text for b in response.content if b.type == "text")
        tool_uses = [b for b in response.content if b.type == "tool_use"]
        turn = Turn(
            text=text, model=response.model,
            input_tokens=usage.input_tokens, output_tokens=usage.output_tokens,
            cache_read_tokens=usage.cache_read_input_tokens or 0,
        )
        if response.stop_reason == "refusal":
            turn.stop_reason = "refusal"
            self.messages.append({"role": "assistant",
                                  "content": text.strip() or "(The model declined to answer this request.)"})
        elif response.stop_reason == "max_tokens" and tool_uses:
            turn.stop_reason = "max_tokens"
            self.messages.append({"role": "assistant",
                                  "content": text.strip() or "(The answer was cut off before a tool call completed.)"})
        else:
            turn.stop_reason = "tool_use" if tool_uses else "end_turn"
            turn.tool_calls = [ToolCall(b.id, b.name, b.input) for b in tool_uses]
            # Keep the native content blocks (including thinking) for replay.
            self.messages.append({"role": "assistant", "content": response.content})
        yield turn


# -- Ollama (open-source models) ----------------------------------------------

class OllamaProvider(Provider):
    name = "ollama"
    extra_system = (
        "Tools are invoked through function calling only. Never write SQL or a tool call "
        "inside your text reply; call run_sql or render_chart instead, then answer from the "
        "result. Call one tool at a time."
    )

    def __init__(self, system: str, tools: list[dict], model: str, client=None,
                 num_ctx: int | None = None, think: bool | None = None,
                 temperature: float = 0.2, **_ignored):
        super().__init__(system, tools, model)
        import ollama

        self._ollama = ollama
        self.client = client or ollama.Client()
        self.num_ctx = num_ctx or int(os.environ.get("OLLAMA_NUM_CTX", "16384"))
        # Thinking on by default: on Qwen 3 8B it turned a wrong multi-step answer into a
        # correct one at the cost of ~20s. Set PODCAST_AGENT_THINK=0 to trade accuracy for speed.
        self.think = think if think is not None else os.environ.get("PODCAST_AGENT_THINK", "1") != "0"
        self.temperature = temperature
        self._tools = [self._convert_tool(t) for t in tools]
        self._call_counter = 0

    @staticmethod
    def _convert_tool(tool: dict) -> dict:
        return {
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": tool["input_schema"],
            },
        }

    def add_tool_results(self, results: list[ToolResult]) -> None:
        for r in results:
            content = r.content if not r.is_error else f"ERROR: {r.content}"
            self.messages.append({"role": "tool", "tool_name": r.name, "content": content})

    def _parse_args(self, raw: object) -> object:
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return raw
        return dict(raw) if isinstance(raw, dict) or hasattr(raw, "items") else raw

    def stream(self) -> Iterator[str | Turn]:
        text, tool_calls, last = "", [], None
        try:
            chunks = self.client.chat(
                model=self.model,
                messages=[{"role": "system", "content": self.system}, *self.messages],
                tools=self._tools,
                stream=True,
                think=self.think,
                options={"num_ctx": self.num_ctx, "temperature": self.temperature},
            )
            for chunk in chunks:
                last = chunk
                msg = chunk.message
                if msg.content:
                    text += msg.content
                    yield msg.content
                for tc in msg.tool_calls or []:
                    self._call_counter += 1
                    tool_calls.append(ToolCall(
                        id=f"call_{self._call_counter}",
                        name=tc.function.name,
                        args=self._parse_args(tc.function.arguments),
                    ))
        except self._ollama.ResponseError as exc:
            raise ProviderError(f"Ollama error: {exc}") from exc
        except Exception as exc:  # connection failures surface as httpx errors
            if exc.__class__.__module__.startswith("httpx"):
                raise ProviderError(f"Cannot reach Ollama ({exc}). Is `ollama serve` running?") from exc
            raise

        done_reason = getattr(last, "done_reason", None) or "stop"
        turn = Turn(
            text=text, tool_calls=tool_calls, model=self.model,
            input_tokens=getattr(last, "prompt_eval_count", None) or 0,
            output_tokens=getattr(last, "eval_count", None) or 0,
        )
        if done_reason == "length" and tool_calls:
            turn.stop_reason = "max_tokens"
            turn.tool_calls = []
            self.messages.append({"role": "assistant", "content": text or "(cut off)"})
        else:
            turn.stop_reason = "tool_use" if tool_calls else "end_turn"
            assistant: dict = {"role": "assistant", "content": text}
            if tool_calls:
                assistant["tool_calls"] = [
                    {"function": {"name": c.name, "arguments": c.args if isinstance(c.args, dict) else {}}}
                    for c in tool_calls
                ]
            self.messages.append(assistant)
        yield turn


def make_provider(name: str, system: str, tools: list[dict], model: str | None = None, **kwargs) -> Provider:
    if name not in PROVIDERS:
        raise ValueError(f"unknown provider {name!r}; choose from {PROVIDERS}")
    cls = AnthropicProvider if name == "anthropic" else OllamaProvider
    return cls(system, tools, model or DEFAULT_MODELS[name], **kwargs)
