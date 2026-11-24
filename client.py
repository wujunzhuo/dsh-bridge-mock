#!/usr/bin/env python3
"""Send a prompt to server over its WebSocket JSON-RPC API."""

import argparse
import asyncio
import json
import os
import sys
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


def paint(text: str, code: str, *, enabled: bool) -> str:
    if not enabled:
        return text
    return f"\033[{code}m{text}\033[0m"


def display_value(value: object) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, indent=2)


def report_event(
    params: object, *, color: bool, verbose: bool
) -> tuple[bool, str | None]:
    """Print a session event and return (printed answer text, event ID)."""
    if not isinstance(params, dict):
        return False, None
    payload = params.get("payload")
    event = payload.get("event") if isinstance(payload, dict) else None
    if not isinstance(event, dict):
        return False, params.get("event_id")

    event_type = event.get("type")
    event_id = params.get("event_id")
    if event_type == "text" and isinstance(event.get("text"), str):
        sys.stdout.write(event["text"])
        sys.stdout.flush()
        return bool(event["text"]), event_id

    if event_type == "thinking" and event.get("text"):
        label = paint("[thinking]", "35", enabled=color)
        print(f"\n{label} {event['text']}", file=sys.stderr)
    elif event_type == "tool_call":
        label = paint("[tool call]", "36", enabled=color)
        print(f"\n{label} {event.get('tool', 'tool')}", file=sys.stderr)
        if event.get("input") is not None:
            print(display_value(event["input"]), file=sys.stderr)
    elif event_type == "tool_result":
        failed = event.get("status") == "error"
        label = paint(
            "[tool error]" if failed else "[tool result]",
            "31" if failed else "32",
            enabled=color,
        )
        print(f"{label} {event.get('status', 'completed')}", file=sys.stderr)
        if event.get("result") is not None:
            print(display_value(event["result"]), file=sys.stderr)
        if event.get("error") is not None:
            print(display_value(event["error"]), file=sys.stderr)
    elif verbose and event_type == "status":
        label = paint("[status]", "90", enabled=color)
        print(f"{label} {event.get('phase', 'working')}", file=sys.stderr)
    return False, event_id


def color_enabled(mode: str) -> bool:
    if mode == "always":
        return True
    if mode == "never" or os.environ.get("NO_COLOR"):
        return False
    return sys.stderr.isatty()


def display_once(
    params: object, *, seen: set[str], color: bool, verbose: bool
) -> bool:
    if isinstance(params, dict):
        event_id = params.get("event_id")
        if isinstance(event_id, str):
            if event_id in seen:
                return False
            seen.add(event_id)
    printed, _ = report_event(params, color=color, verbose=verbose)
    return printed


async def replay_session_history(
    socket, args: argparse.Namespace, *, color: bool
) -> set[str]:
    """Fetch and print existing events before following this session live."""
    request_id = f"attachment-{uuid.uuid4()}"
    await socket.send(json.dumps({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "session.attachment",
        "params": {"after_event_id": args.after_event_id},
    }))

    seen: set[str] = set()
    buffered: list[object] = []
    while True:
        frame = json.loads(await socket.recv())
        if frame.get("method") == "session.event":
            buffered.append(frame.get("params"))
            continue

        if frame.get("id") != request_id:
            continue
        if "error" in frame:
            error = frame["error"]
            message = error.get("message", str(error)) if isinstance(error, dict) else str(error)
            raise RuntimeError(message)

        events = (frame.get("result") or {}).get("events", [])
        for event in events:
            display_once(event, seen=seen, color=color, verbose=args.verbose)
        for event in buffered:
            display_once(event, seen=seen, color=color, verbose=args.verbose)
        return seen


async def read_stdin_line() -> str | None:
    """Read one terminal line without blocking websocket event handling."""
    loop = asyncio.get_running_loop()
    try:
        fd = sys.stdin.fileno()
    except (AttributeError, OSError):
        line = await asyncio.to_thread(sys.stdin.readline)
        return line.rstrip("\r\n") if line else None

    future: asyncio.Future[str | None] = loop.create_future()
    content = bytearray()

    def readable() -> None:
        try:
            chunk = os.read(fd, 1)
        except OSError as error:
            loop.remove_reader(fd)
            if not future.done():
                future.set_exception(error)
            return

        if not chunk:
            loop.remove_reader(fd)
            if not future.done():
                value = content.decode(errors="replace") if content else None
                future.set_result(value)
            return
        if chunk == b"\n":
            loop.remove_reader(fd)
            if not future.done():
                future.set_result(content.decode(errors="replace").rstrip("\r"))
            return
        content.extend(chunk)

    try:
        loop.add_reader(fd, readable)
    except (NotImplementedError, OSError, RuntimeError):
        line = await asyncio.to_thread(sys.stdin.readline)
        return line.rstrip("\r\n") if line else None
    try:
        return await future
    finally:
        loop.remove_reader(fd)


async def run_session(
    socket,
    args: argparse.Namespace,
    *,
    color: bool,
    initial_prompt: str | None,
    seen: set[str],
) -> None:
    """Run prompts and receive session events on the same websocket."""
    pending: dict[str, bool] = {}
    close_request_id: str | None = None

    async def send_prompt(text: str) -> None:
        request_id = f"prompt-{uuid.uuid4()}"
        pending[request_id] = False
        await socket.send(json.dumps({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "session.prompt",
            "params": {"prompt": text},
        }))

    if initial_prompt:
        await send_prompt(initial_prompt)

    allow_input = not args.once and (sys.stdin.isatty() or args.interactive)
    watch_only = args.session_id is not None and initial_prompt is None
    input_task = None
    if allow_input:
        print(
            "Interactive session: enter follow-ups/approvals, or /quit to destroy it.",
            file=sys.stderr,
        )
        label = paint("[you] >", "1;34", enabled=color)
        sys.stderr.write(f"\n{label} ")
        sys.stderr.flush()
        input_task = asyncio.create_task(read_stdin_line())

    receive_task = asyncio.create_task(socket.recv())
    try:
        while True:
            if not pending and not allow_input and not watch_only:
                return

            waiting = {receive_task}
            if input_task is not None:
                waiting.add(input_task)
            done, _ = await asyncio.wait(
                waiting,
                timeout=args.timeout if pending else None,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done and pending:
                raise TimeoutError(f"no server response for {args.timeout:g} seconds")

            if receive_task in done:
                frame = json.loads(receive_task.result())
                receive_task = asyncio.create_task(socket.recv())

                # A newly-created service session sends its ID as an init response.
                if frame.get("id") == "init" and isinstance(frame.get("result"), dict):
                    assigned_id = frame["result"].get("session_id")
                    if assigned_id:
                        print(f"session_id: {assigned_id}", file=sys.stderr)
                elif frame.get("method") == "session.closed":
                    print("\nSession destroyed.", file=sys.stderr)
                    return
                elif frame.get("method") == "session.event":
                    params = frame.get("params")
                    was_text = display_once(
                        params, seen=seen, color=color, verbose=args.verbose
                    )
                    if was_text and pending:
                        # Prompts are processed serially by the backend.
                        first_pending = next(iter(pending))
                        pending[first_pending] = True
                elif frame.get("id") in pending:
                    request_id = frame["id"]
                    streamed = pending.pop(request_id)
                    if "error" in frame:
                        error = frame["error"]
                        message = error.get("message", str(error)) if isinstance(
                            error, dict
                        ) else str(error)
                        raise RuntimeError(message)
                    else:
                        result = frame.get("result") or {}
                        if result.get("ok") is False:
                            raise RuntimeError("agent request failed")
                        else:
                            answer = result.get("answer")
                            if isinstance(answer, str) and answer and not streamed:
                                print(answer, end="" if answer.endswith("\n") else "\n")
                            elif streamed:
                                print()
                elif frame.get("id") == close_request_id:
                    if "error" in frame:
                        error = frame["error"]
                        message = error.get("message", str(error)) if isinstance(
                            error, dict
                        ) else str(error)
                        raise RuntimeError(message)
                    print("\nSession destroyed.", file=sys.stderr)
                    return

            if input_task is not None and input_task in done:
                line = input_task.result()
                if line is None:
                    input_task = None
                    allow_input = False
                else:
                    if line.strip() == "/quit":
                        close_request_id = f"close-{uuid.uuid4()}"
                        await socket.send(json.dumps({
                            "jsonrpc": "2.0",
                            "id": close_request_id,
                            "method": "session.close",
                            "params": {},
                        }))
                        input_task = None
                        allow_input = False
                    elif line.strip():
                        await send_prompt(line.strip())
                    if allow_input and input_task is not None:
                        label = paint("[you] >", "1;34", enabled=color)
                        sys.stderr.write(f"\n{label} ")
                        sys.stderr.flush()
                        input_task = asyncio.create_task(read_stdin_line())

    finally:
        receive_task.cancel()
        if input_task is not None:
            input_task.cancel()
        await asyncio.gather(
            receive_task,
            *([input_task] if input_task is not None else []),
            return_exceptions=True,
        )


async def run(args: argparse.Namespace) -> None:
    prompt = args.prompt
    if (
        prompt is None
        and not sys.stdin.isatty()
        and not (args.session_id and args.interactive)
    ):
        prompt = sys.stdin.read().strip()
    if not prompt and not args.session_id:
        raise ValueError(
            "provide a prompt argument or pipe prompt text to stdin")

    url = build_url(args.url, args.api_key, args.session_id)
    color = color_enabled(args.color)
    if args.session_id:
        print(f"session_id: {args.session_id}", file=sys.stderr)

    try:
        import websockets
    except ImportError as error:
        raise RuntimeError(
            "websockets is required; install it with: "
            "python3 -m pip install -r requirements.txt"
        ) from error

    async with websockets.connect(url) as socket:
        seen: set[str] = set()
        if not prompt and args.session_id:
            print("Attached to session; loading history.", file=sys.stderr)
            seen = await replay_session_history(socket, args, color=color)
        await run_session(
            socket,
            args,
            color=color,
            initial_prompt=prompt,
            seen=seen,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an agent prompt through dsh-bridge-mock.")
    parser.add_argument(
        "prompt", nargs="?",
        help="prompt to send (if omitted, read from stdin)"
    )
    parser.add_argument(
        "--url", default="ws://127.0.0.1:8080/api/remote.mux",
        help="server WebSocket URL (default: %(default)s)",
    )
    parser.add_argument(
        "--api-key", default=os.environ.get("DSH_API_KEY"),
        help="tenant API key (or set DSH_API_KEY)",
    )
    parser.add_argument(
        "--session_id", "--session-id", dest="session_id",
        help="attach to a bridge session (without a prompt, listen to it)",
    )
    parser.add_argument(
        "--after-event-id",
        help="when listening, replay events after this event ID",
    )
    parser.add_argument(
        "--timeout", type=float, default=300,
        help="response timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="also print status events"
    )
    parser.add_argument(
        "--interactive", action="store_true",
        help="read follow-up prompts from stdin even when it is not a terminal",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="send the initial prompt and exit after its response",
    )
    parser.add_argument(
        "--color", choices=("auto", "always", "never"), default="auto",
        help="colored event labels (default: %(default)s)",
    )
    args = parser.parse_args()
    if not args.api_key:
        parser.error("--api-key is required (or set DSH_API_KEY)")
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    if args.interactive and args.once:
        parser.error("--interactive and --once cannot be used together")
    return args


def main() -> int:
    args = parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
