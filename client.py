#!/usr/bin/env python3
"""Interactive terminal client for the dsh-bridge-mock WebSocket API."""

import argparse
import asyncio
import curses
import json
import os
import sys
import time
import unicodedata
import uuid
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def build_url(endpoint: str, api_key: str, session_id: str | None) -> str:
    parts = urlsplit(endpoint)
    if parts.scheme not in ("ws", "wss") or not parts.netloc:
        raise ValueError("WebSocket URL must start with ws:// or wss://")
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query)
        if key not in {"api_key", "session_id"}
    ]
    query.append(("api_key", api_key))
    if session_id:
        query.append(("session_id", session_id))
    return urlunsplit((parts.scheme, parts.netloc, parts.path,
                       urlencode(query), parts.fragment))


def char_width(char: str) -> int:
    if unicodedata.combining(char):
        return 0
    return 2 if unicodedata.east_asian_width(char) in ("W", "F") else 1


def wrap_cells(text: str, width: int) -> list[str]:
    width = max(1, width)
    lines = [""]
    used = 0
    for char in text:
        if char == "\n":
            lines.append("")
            used = 0
            continue
        if not char.isprintable() and char != "\t":
            char = " "
        if char == "\t":
            char = " "
        size = char_width(char)
        if used + size > width and lines[-1]:
            lines.append("")
            used = 0
        lines[-1] += char
        used += size
    return lines


def clip_cells(text: str, width: int) -> str:
    result = []
    used = 0
    for char in text:
        size = char_width(char)
        if used + size > width:
            break
        result.append(char)
        used += size
    return "".join(result)


class TerminalUI:
    LABELS = {
        "user": "You: ",
        "assistant": "Agent: ",
        "thinking": "Thinking: ",
        "tool_call": "Tool call: ",
        "tool_result": "Tool result: ",
        "status": "Status: ",
        "system": "Info: ",
        "error": "Error: ",
    }

    def __init__(self, screen):
        self.screen = screen
        self.screen.keypad(True)
        self.screen.nodelay(True)
        self.session_id = "connecting"
        self.status = "connected"
        self.active_turn_id: str | None = None
        self.active_turn_has_text = False
        self.entries: list[tuple[str, str]] = []
        self.input = ""
        self.scroll_offset = 0
        self.assistant_open = False
        self.colors = False
        self.pairs: dict[str, int] = {}
        # Leave mouse input to the terminal so normal click-and-drag selection works.
        try:
            curses.mousemask(0)
        except curses.error:
            pass
        try:
            curses.curs_set(1)
        except curses.error:
            pass
        if curses.has_colors():
            try:
                curses.start_color()
                curses.use_default_colors()
                colors = {
                    "user": curses.COLOR_CYAN,
                    "assistant": curses.COLOR_GREEN,
                    "thinking": curses.COLOR_MAGENTA,
                    "tool_call": curses.COLOR_CYAN,
                    "tool_result": curses.COLOR_GREEN,
                    "status": curses.COLOR_YELLOW,
                    "system": curses.COLOR_BLUE,
                    "error": curses.COLOR_RED,
                }
                for index, (name, color) in enumerate(colors.items(), start=1):
                    curses.init_pair(index, color, -1)
                    self.pairs[name] = curses.color_pair(index)
                self.colors = True
            except curses.error:
                self.pairs.clear()

    def add(self, kind: str, text: str) -> None:
        self.entries.append((kind, str(text)))
        self.assistant_open = False
        if len(self.entries) > 2000:
            del self.entries[:500]
        self.scroll_offset = 0

    def append_assistant(self, text: str) -> None:
        if not self.assistant_open or not self.entries or self.entries[-1][0] != "assistant":
            self.entries.append(("assistant", text))
            self.assistant_open = True
        else:
            kind, previous = self.entries[-1]
            self.entries[-1] = (kind, previous + text)
        if len(self.entries) > 2000:
            del self.entries[:500]
        self.scroll_offset = 0

    def finish_turn(self) -> None:
        self.assistant_open = False

    def scroll(self, lines: int) -> None:
        rows, cols = self.screen.getmaxyx()
        output_height = max(1, rows - 4)
        max_offset = max(0, len(self._output_lines(cols - 1)) - output_height)
        self.scroll_offset = min(max(0, self.scroll_offset + lines), max_offset)

    def _output_lines(self, width: int) -> list[tuple[str, str]]:
        lines = []
        for kind, text in self.entries:
            content = self.LABELS.get(kind, "") + text
            lines.extend((kind, line) for line in wrap_cells(content, width))
        return lines

    def _write(self, row: int, col: int, text: str, attr: int = 0) -> None:
        rows, cols = self.screen.getmaxyx()
        if row < 0 or row >= rows or col < 0 or col >= cols:
            return
        try:
            self.screen.addstr(row, col, clip_cells(text, cols - col - 1), attr)
        except curses.error:
            pass

    def draw(self) -> None:
        self.screen.erase()
        rows, cols = self.screen.getmaxyx()
        if rows < 4 or cols < 10:
            self._write(0, 0, "Resize terminal to continue")
            self.screen.refresh()
            return

        title = (
            f" dsh agent · session {self.session_id} · {self.status} "
            "· PgUp/PgDn scroll · terminal mouse selection "
        )
        self._write(0, 0, clip_cells(title, cols - 1), curses.A_REVERSE)

        separator = rows - 3
        output_height = max(1, separator - 1)
        output = self._output_lines(cols - 1)
        end = max(0, len(output) - self.scroll_offset)
        start = max(0, end - output_height)
        visible = output[start:end]
        for index, (kind, text) in enumerate(visible):
            self._write(index + 1, 0, text, self.pairs.get(kind, 0))

        try:
            self.screen.hline(separator, 0, curses.ACS_HLINE, cols)
        except curses.error:
            pass
        self._write(rows - 2, 0, " Enter sends · /stop cancels · /clear resets · /quit exits ", curses.A_BOLD)

        input_prefix = "> "
        available = max(1, cols - len(input_prefix) - 2)
        shown = self.input
        while shown and sum(char_width(char) for char in shown) > available:
            shown = shown[1:]
        self._write(rows - 1, 0, input_prefix + shown, curses.A_BOLD)
        cursor_col = min(cols - 2, len(input_prefix) + sum(char_width(char) for char in shown))
        try:
            self.screen.move(rows - 1, cursor_col)
        except curses.error:
            pass
        self.screen.refresh()

    def key(self):
        try:
            return self.screen.get_wch()
        except curses.error:
            return None


def event_to_ui(params: object, ui: TerminalUI) -> bool:
    if not isinstance(params, dict):
        return False
    payload = params.get("payload")
    event = payload.get("event") if isinstance(payload, dict) else None
    if not isinstance(event, dict):
        return False

    event_type = event.get("type")
    if event_type == "text" and isinstance(event.get("text"), str):
        ui.append_assistant(event["text"])
        ui.active_turn_has_text = ui.active_turn_id is not None
        return bool(event["text"])
    if event_type == "thinking" and event.get("text"):
        ui.add("thinking", event["text"])
    elif event_type == "tool_call":
        details = event.get("tool", "tool")
        if event.get("input") is not None:
            details += "\n" + display_value(event["input"])
        ui.add("tool_call", details)
    elif event_type == "tool_result":
        status = event.get("status", "completed")
        kind = "error" if status == "error" else "tool_result"
        details = str(status)
        if event.get("result") is not None:
            details += "\n" + display_value(event["result"])
        if event.get("error") is not None:
            details += "\n" + display_value(event["error"])
        ui.add(kind, details)
    elif event_type == "status":
        ui.add("status", event.get("phase", "working"))
    elif event_type == "turn_status":
        status = event.get("status")
        if status == "started":
            ui.active_turn_id = payload.get("turn_id")
            ui.active_turn_has_text = False
            ui.status = "turn running"
        elif status == "completed":
            answer = event.get("answer")
            if isinstance(answer, str) and answer and not ui.active_turn_has_text:
                ui.append_assistant(answer)
            ui.status = "turn completed naturally"
            ui.add("status", "Turn completed naturally.")
            ui.active_turn_id = None
            ui.active_turn_has_text = False
            ui.finish_turn()
            return isinstance(answer, str) and bool(answer)
        elif status == "cancelled":
            ui.status = "turn cancelled"
            ui.add("status", "Turn cancelled by client.")
            ui.active_turn_id = None
            ui.active_turn_has_text = False
            ui.finish_turn()
        elif status == "failed":
            ui.status = "turn failed"
            ui.active_turn_id = None
            ui.active_turn_has_text = False
            ui.finish_turn()
    return False


def display_value(value: object) -> str:
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2)


def display_once(params: object, *, seen: set[str], ui: TerminalUI) -> bool:
    if isinstance(params, dict):
        event_id = params.get("event_id")
        if isinstance(event_id, str):
            if event_id in seen:
                return False
            seen.add(event_id)
    return event_to_ui(params, ui)


async def replay_session_history(socket, args, ui: TerminalUI) -> set[str]:
    request_id = f"attachment-{uuid.uuid4()}"
    await socket.send(json.dumps({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "session.attachment",
        "params": {"after_event_id": args.after_event_id},
    }))

    seen: set[str] = set()
    buffered = []
    while True:
        frame = json.loads(await socket.recv())
        if frame.get("method") == "session.event":
            buffered.append(frame.get("params"))
            continue
        if frame.get("id") != request_id:
            continue
        if "error" in frame:
            raise RuntimeError(frame["error"].get("message", str(frame["error"])))
        events = (frame.get("result") or {}).get("events", [])
        for event in [*events, *buffered]:
            display_once(event, seen=seen, ui=ui)
        return seen


async def run_session(socket, args, ui: TerminalUI, seen: set[str]) -> str:
    pending: dict[str, tuple[bool, float]] = {}
    cancel_pending: set[str] = set()
    close_request_id = None
    clearing = False

    async def send_prompt(text: str) -> None:
        request_id = f"prompt-{uuid.uuid4()}"
        pending[request_id] = (False, asyncio.get_running_loop().time())
        ui.add("user", text)
        await socket.send(json.dumps({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "session.prompt",
            "params": {"prompt": text},
        }))

    receive_task = asyncio.create_task(socket.recv())
    try:
        while True:
            key = ui.key()
            while key is not None:
                if isinstance(key, int):
                    rows = ui.screen.getmaxyx()[0]
                    if key == curses.KEY_PPAGE:
                        ui.scroll(max(1, rows - 5))
                    elif key == curses.KEY_NPAGE:
                        ui.scroll(-max(1, rows - 5))
                    elif key == curses.KEY_UP:
                        ui.scroll(1)
                    elif key == curses.KEY_DOWN:
                        ui.scroll(-1)
                    elif key in (curses.KEY_BACKSPACE, curses.KEY_DC):
                        ui.input = ui.input[:-1]
                elif key in ("\n", "\r"):
                    value = ui.input.strip()
                    ui.input = ""
                    if value == "/quit":
                        return "quit"
                    elif value == "/stop":
                        if ui.active_turn_id:
                            cancel_id = f"cancel-{uuid.uuid4()}"
                            cancel_pending.add(cancel_id)
                            await socket.send(json.dumps({
                                "jsonrpc": "2.0",
                                "id": cancel_id,
                                "method": "session.cancel",
                                "params": {"turn_id": ui.active_turn_id},
                            }))
                            ui.status = "cancellation requested"
                        else:
                            ui.add("system", "No active turn to cancel.")
                    elif value == "/clear" and not clearing:
                        close_request_id = f"close-{uuid.uuid4()}"
                        clearing = True
                        ui.status = "clearing session"
                        await socket.send(json.dumps({
                            "jsonrpc": "2.0",
                            "id": close_request_id,
                            "method": "session.close",
                            "params": {},
                        }))
                    elif value and not clearing:
                        await send_prompt(value)
                elif key in ("\x7f", "\b"):
                    ui.input = ui.input[:-1]
                elif isinstance(key, str) and key.isprintable() and not clearing:
                    ui.input += key
                key = ui.key()

            done, _ = await asyncio.wait({receive_task}, timeout=0.05)
            if done:
                frame = json.loads(receive_task.result())
                receive_task = asyncio.create_task(socket.recv())

                if frame.get("id") == "init" and isinstance(frame.get("result"), dict):
                    ui.session_id = frame["result"].get("session_id", ui.session_id)
                    ui.status = "ready"
                elif frame.get("method") == "session.closed":
                    if clearing:
                        return "clear"
                    ui.add("system", "Session was cleared by the server.")
                    ui.draw()
                    return "closed"
                elif frame.get("method") == "session.event":
                    params = frame.get("params")
                    payload = params.get("payload") if isinstance(params, dict) else None
                    event = payload.get("event") if isinstance(payload, dict) else None
                    if isinstance(payload, dict) and isinstance(event, dict) and event.get("type") == "turn_status":
                        if event.get("status") == "started":
                            ui.active_turn_id = payload.get("turn_id")
                        elif event.get("status") in {"completed", "cancelled", "failed"}:
                            ui.active_turn_id = None
                    if display_once(params, seen=seen, ui=ui) and pending:
                        request_id = next(iter(pending))
                        _, started = pending[request_id]
                        pending[request_id] = (True, started)
                elif frame.get("id") in pending:
                    request_id = frame["id"]
                    streamed, _ = pending.pop(request_id)
                    if "error" in frame:
                        ui.add("error", frame["error"].get("message", str(frame["error"])))
                    else:
                        result = frame.get("result") or {}
                        if result.get("cancelled"):
                            ui.active_turn_id = None
                            ui.status = "turn cancelled"
                            if not any(kind == "status" and text == "Turn cancelled by client."
                                       for kind, text in ui.entries[-3:]):
                                ui.add("system", "Turn cancelled.")
                        elif result.get("ok") is False:
                            ui.add("error", "Agent request failed")
                        elif result.get("answer") and not streamed:
                            ui.append_assistant(result["answer"])
                    ui.finish_turn()
                elif frame.get("id") in cancel_pending:
                    cancel_pending.discard(frame["id"])
                    if "error" in frame:
                        ui.add("error", frame["error"].get("message", str(frame["error"])))
                    elif not (frame.get("result") or {}).get("accepted"):
                        ui.add("system", "The turn has already ended.")
                elif frame.get("id") == close_request_id and close_request_id:
                    if "error" in frame:
                        ui.add("error", frame["error"].get("message", str(frame["error"])))
                        clearing = False
                        close_request_id = None
                        ui.status = "ready"
                    else:
                        return "clear"

            now = asyncio.get_running_loop().time()
            expired = [
                request_id
                for request_id, (_, started) in pending.items()
                if now - started > args.timeout
            ]
            for request_id in expired:
                pending.pop(request_id, None)
                ui.add("error", f"No response for {args.timeout:g} seconds")
            ui.draw()
    finally:
        receive_task.cancel()
        await asyncio.gather(receive_task, return_exceptions=True)


async def run(args, ui: TerminalUI) -> None:
    try:
        import websockets
    except ImportError as error:
        raise RuntimeError("websockets is required; install it with: uv pip install websockets") from error

    session_id = args.session_id
    while True:
        async with websockets.connect(build_url(args.url, args.api_key, session_id)) as socket:
            seen: set[str] = set()
            if session_id:
                ui.session_id = session_id
                ui.status = "loading session history"
                seen = await replay_session_history(socket, args, ui)
            outcome = await run_session(socket, args, ui, seen)

        if outcome != "clear":
            return
        session_id = None
        ui.entries.clear()
        ui.assistant_open = False
        ui.input = ""
        ui.scroll_offset = 0
        ui.session_id = "connecting"
        ui.status = "starting new session"
        ui.add("system", "Session cleared. Starting a new session.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Interactive dsh-bridge-mock client.")
    parser.add_argument("--url", default="ws://127.0.0.1:8080/api/remote.mux")
    parser.add_argument("--api-key", default=os.environ.get("DSH_API_KEY"))
    parser.add_argument("--session_id", "--session-id", dest="session_id")
    parser.add_argument("--after-event-id", help="replay events after this event ID when attaching")
    parser.add_argument("--timeout", type=float, default=300, help="request timeout in seconds")
    args = parser.parse_args()
    if not args.api_key:
        parser.error("--api-key is required (or set DSH_API_KEY)")
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    return args


def main() -> int:
    args = parse_args()
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        print("client.py requires an interactive terminal", file=sys.stderr)
        return 2
    try:
        def start(screen):
            ui = TerminalUI(screen)
            asyncio.run(run(args, ui))

        curses.wrapper(start)
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
