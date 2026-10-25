import asyncio
import sys
lazy import traceback


def _exception_text(error):
    return "".join(traceback.format_exception(error)).strip()


async def _lifespan_result(application, scope, receive, send):
    # Refusing lifespan before its first message is a supported ASGI outcome.
    # Inspect it at the protocol boundary, not as a TaskGroup sibling failure.
    try:
        await application(scope, receive, send)
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        return error
    return None


async def _lifespan_message(task, outgoing):
    # This short-lived group owns only the queue waiter, never the lifespan
    # application or requests. Python 3.15 cancel() joins a losing waiter without
    # an orphan task or an artificial exception used to terminate the group.
    async with asyncio.TaskGroup() as phase:
        message = phase.create_task(outgoing.get())
        done, _ = await asyncio.wait(
            (task, message), return_when=asyncio.FIRST_COMPLETED
        )
        received = message in done
        result = message.result() if received else None
        phase.cancel()
    return received, result


async def drive_http(application, exchange, state, waiters):
    response_complete = False

    # The body as the ingress reads it, a part per message; once it is all
    # read, the next receive waits for the reply to complete or the client to
    # leave, and then says `http.disconnect`. Waits are resolved by the loop,
    # as a WebSocket's are.
    async def receive():
        event = exchange.try_receive()
        if event is None:
            waiter = asyncio.get_running_loop().create_future()
            waiters[exchange.id] = (exchange, waiter)
            try:
                event = await waiter
            finally:
                waiters.pop(exchange.id, None)
        return event

    async def send(message):
        nonlocal response_complete
        message_type = message.get("type")
        if message_type == "http.response.start":
            headers = [
                (bytes(name), bytes(value))
                for name, value in message.get("headers", [])
            ]
            exchange.response_start(message["status"], headers)
            return
        if message_type == "http.response.body":
            more_body = bool(message.get("more_body", False))
            waiting = exchange.response_body(bytes(message.get("body", b"")), more_body)
            if waiting is not None:
                await waiting
            response_complete = not more_body
            if response_complete:
                # A receive waiting past the body learns the exchange is over.
                entry = waiters.get(exchange.id)
                if entry is not None and not entry[1].done():
                    event = exchange.try_receive()
                    if event is not None:
                        entry[1].set_result(event)
            return
        raise RuntimeError(f"unexpected ASGI HTTP message: {message_type!r}")

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.5"},
        "http_version": exchange.http_version,
        "method": exchange.method,
        "scheme": exchange.scheme,
        "path": exchange.path,
        "raw_path": bytes(exchange.raw_path),
        "query_string": bytes(exchange.query_string),
        "root_path": "",
        "headers": [
            (bytes(name), bytes(value)) for name, value in exchange.headers
        ],
        "client": exchange.client,
        "server": exchange.server,
        "state": state.copy(),
    }
    try:
        await application(scope, receive, send)
        if not response_complete:
            raise RuntimeError("ASGI application returned before completing the HTTP response")
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        exchange.fail(_exception_text(error))


async def drive_websocket(application, socket, state, waiters):
    headers = [(bytes(name), bytes(value)) for name, value in socket.headers]
    subprotocols = []
    for name, value in headers:
        if name.lower() == b"sec-websocket-protocol":
            subprotocols += [
                part.strip() for part in value.decode("latin-1").split(",") if part.strip()
            ]
    scope = {
        "type": "websocket",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": socket.http_version,
        "scheme": "wss" if socket.scheme == "https" else "ws",
        "path": socket.path,
        "raw_path": bytes(socket.raw_path),
        "query_string": bytes(socket.query_string),
        "root_path": "",
        "headers": headers,
        "client": socket.client,
        "server": socket.server,
        "subprotocols": subprotocols,
        "state": state.copy(),
        "extensions": {
            "gufi.transport": {
                "name": socket.transport,
                "method": socket.method,
                "scheme": socket.scheme,
                "http_version": socket.http_version,
            }
        },
    }
    connected = False

    async def receive():
        nonlocal connected
        if not connected:
            connected = True
            return {"type": "websocket.connect"}
        event = socket.try_receive()
        if event is None:
            waiter = asyncio.get_running_loop().create_future()
            waiters[socket.id] = (socket, waiter)
            try:
                event = await waiter
            finally:
                waiters.pop(socket.id, None)
        return event

    async def send(message):
        kind = message.get("type")
        if kind == "websocket.accept":
            socket.accept(
                message.get("subprotocol"),
                [(bytes(name), bytes(value)) for name, value in message.get("headers", [])],
            )
        elif kind == "websocket.send":
            waiting = socket.send(message.get("text"), message.get("bytes"))
            if waiting is not None:
                await waiting
        elif kind == "websocket.close":
            waiting = socket.close(int(message.get("code", 1000)), message.get("reason") or "")
            if waiting is not None:
                await waiting
        else:
            raise RuntimeError(f"unexpected ASGI websocket message: {kind!r}")

    try:
        await application(scope, receive, send)
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        socket.fail(_exception_text(error))
    finally:
        socket.finish()


async def _drive_exchange(application, exchange, state, waiters):
    # Contain even scope construction and transport-cleanup errors. An exchange
    # failure must not trigger TaskGroup's sibling-cancellation semantics.
    try:
        if exchange.kind == "websocket":
            await drive_websocket(application, exchange, state, waiters)
        else:
            await drive_http(application, exchange, state, waiters)
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        try:
            exchange.fail(_exception_text(error))
        except BaseException as reporting_error:
            asyncio.get_running_loop().call_exception_handler({
                "message": "ASGI exchange failure reporting failed",
                "exception": reporting_error,
            })
    finally:
        entry = waiters.pop(exchange.id, None)
        if entry is not None and not entry[1].done():
            entry[1].cancel()


async def _serve_requests(application, control, state, waiters):
    loop = asyncio.get_running_loop()
    finished = loop.create_future()
    doorbell = control.doorbell()

    try:
        async with asyncio.TaskGroup() as requests:
            def answer():
                exchanges, ready, last = control.take_exchanges()
                for socket_id in ready:
                    entry = waiters.pop(socket_id, None)
                    if entry is None:
                        continue
                    socket, waiter = entry
                    if waiter.done():
                        continue
                    try:
                        event = socket.try_receive()
                    except BaseException as error:
                        waiter.set_exception(error)
                        continue
                    if event is None:
                        waiters[socket_id] = entry
                    else:
                        waiter.set_result(event)
                for exchange in exchanges:
                    requests.create_task(
                        _drive_exchange(application, exchange, state, waiters)
                    )
                if last and not finished.done():
                    finished.set_result(None)

            loop.add_reader(doorbell, answer)
            answer()
            await finished
            # Normal exit joins all accepted work without cancelling it. The
            # reader stays installed during this join for body/message wakeups.
    finally:
        loop.remove_reader(doorbell)


async def _drive(application, control, lifecycle):
    incoming = asyncio.Queue()
    outgoing = asyncio.Queue()

    async def receive():
        return await incoming.get()

    async def send(message):
        await outgoing.put(message)

    state = {}
    scope = {
        "type": "lifespan",
        "asgi": {"version": "3.0", "spec_version": "2.0"},
        "state": state,
    }
    task = lifecycle.create_task(_lifespan_result(application, scope, receive, send))
    await incoming.put({"type": "lifespan.startup"})
    lifespan, message = await _lifespan_message(task, outgoing)

    # An application that ends the lifespan scope without answering does not
    # implement the protocol. That is allowed, and it is how most of them say
    # so: Django raises "can only handle ASGI/HTTP connections". Refusing is
    # not failing — what fails is `lifespan.startup.failed`, below, which an
    # application sends only after choosing to speak the protocol.
    if not lifespan:
        error = task.result()
        control.lifespan_absent(
            _exception_text(error)
            if error is not None
            else "the application returned without sending a lifespan message"
        )
    else:
        message_type = message.get("type")
        if message_type == "lifespan.startup.failed":
            detail = message.get("message") or "ASGI lifespan startup failed"
            control.startup_failed(detail)
            raise RuntimeError(detail)
        if message_type != "lifespan.startup.complete":
            detail = f"unexpected ASGI lifespan startup message: {message_type!r}"
            control.startup_failed(detail)
            raise RuntimeError(detail)

    control.startup_complete()

    # The loop takes requests itself: the doorbell is one more descriptor it
    # watches, and each time it is readable every request waiting is taken at
    # once. No second Python thread asks for work, so the GIL has one owner.
    # WebSockets waiting for a message and HTTP requests waiting for a part
    # of their body, by id: the doorbell names them and the loop resolves
    # their waits here, on its own thread.
    waiters = {}
    # Django Channels' layer, when gufi installed it, waits in the same table.
    channel_layer = sys.modules.get("gufi.channels")
    if channel_layer is not None:
        channel_layer._waiters = waiters

    try:
        await _serve_requests(application, control, state, waiters)
    finally:
        if channel_layer is not None and channel_layer._waiters is waiters:
            channel_layer._waiters = None

    # An application that never spoke the protocol has nothing to be told the
    # process is ending, and the scope it refused is already over.
    if not lifespan:
        return

    await incoming.put({"type": "lifespan.shutdown"})
    received, message = await _lifespan_message(task, outgoing)
    if not received:
        error = task.result()
        if error is not None:
            raise error
        raise RuntimeError(
            "ASGI application returned before lifespan shutdown completed"
        )

    message_type = message.get("type")
    if message_type == "lifespan.shutdown.failed":
        detail = message.get("message") or "ASGI lifespan shutdown failed"
        raise RuntimeError(detail)
    if message_type != "lifespan.shutdown.complete":
        raise RuntimeError(
            f"unexpected ASGI lifespan shutdown message: {message_type!r}"
        )

    error = await task
    if error is not None:
        raise error


async def drive(application, control):
    try:
        async with asyncio.TaskGroup() as lifecycle:
            await _drive(application, control, lifecycle)
    except BaseExceptionGroup as errors:
        # Our lifespan task returns its exception as a protocol result. Keep a
        # single driver failure's original type/message for the Rust boundary.
        if len(errors.exceptions) == 1:
            raise errors.exceptions[0] from None
        raise
