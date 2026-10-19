"""The shipped ASGI driver, without a Rust runtime, listener or interpreter boot."""

import importlib.util
import asyncio
from collections import deque
from pathlib import Path
import socket
from types import SimpleNamespace
import sys
import unittest

DRIVER = Path(__file__).resolve().parents[2] / "src" / "asgi" / "driver.py"
spec = importlib.util.spec_from_file_location("gufi_test_driver", DRIVER)
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


class Exchange:
    id = 1
    kind = "http"
    http_version = "2"
    method = "POST"
    scheme = "https"
    path = "/example"
    raw_path = b"/example"
    query_string = b""
    headers = [(b"host", b"example.test")]
    client = ("192.0.2.1", 1234)
    server = ("192.0.2.2", 443)

    def __init__(self):
        self.events = []
        self.error = None

    def try_receive(self):
        return {"type": "http.request", "body": b"request", "more_body": False}

    def response_start(self, status, headers):
        self.events.append((status, headers))

    def response_body(self, body, more_body):
        self.events.append((body, more_body))

    def fail(self, error):
        self.error = error


class PendingExchange(Exchange):
    def __init__(self, identity):
        super().__init__()
        self.id = identity
        self.path = f"/{identity}"
        self.incoming = deque()
        self.waiting = asyncio.Event()
        self.completed = asyncio.Event()

    def try_receive(self):
        if self.incoming:
            event = self.incoming.popleft()
            if isinstance(event, BaseException):
                raise event
            return event
        self.waiting.set()
        return None

    def response_body(self, body, more_body):
        super().response_body(body, more_body)
        if not more_body:
            self.completed.set()

    def fail(self, error):
        super().fail(error)
        self.completed.set()


class Control:
    def __init__(self):
        self.reader, self.writer = socket.socketpair()
        self.reader.setblocking(False)
        self.batches = deque()
        self.started = asyncio.Event()
        self.stopping = asyncio.Event()
        self.failures = []
        self.absent = []

    def close(self):
        self.reader.close()
        self.writer.close()

    def doorbell(self):
        return self.reader.fileno()

    def startup_complete(self):
        self.started.set()

    def startup_failed(self, detail):
        self.failures.append(detail)

    def lifespan_absent(self, detail):
        self.absent.append(detail)

    def push(self, exchanges=(), ready=(), last=False):
        self.batches.append((list(exchanges), list(ready), last))
        self.writer.send(b"x")

    def take_exchanges(self):
        try:
            while self.reader.recv(4096):
                pass
        except BlockingIOError:
            pass
        exchanges, ready, last = [], [], False
        while self.batches:
            batch, signals, stop = self.batches.popleft()
            exchanges.extend(batch)
            ready.extend(signals)
            last |= stop
        if last:
            self.stopping.set()
        return exchanges, ready, last


class SocketExchange(PendingExchange):
    kind = "websocket"

    def accept(self, subprotocol, headers):
        self.events.append(("accepted", subprotocol))

    def close(self, code, reason):
        self.events.append(("closed", code, reason))

    def finish(self):
        self.completed.set()


class DriverTests(unittest.IsolatedAsyncioTestCase):
    def control(self):
        control = Control()
        self.addCleanup(control.close)
        return control

    async def start_driver(self, control, http, shutdown=None):
        async def app(scope, receive, send):
            if scope["type"] == "lifespan":
                self.assertEqual((await receive())["type"], "lifespan.startup")
                await send({"type": "lifespan.startup.complete"})
                self.assertEqual((await receive())["type"], "lifespan.shutdown")
                if shutdown is not None:
                    shutdown()
                await send({"type": "lifespan.shutdown.complete"})
            else:
                await http(scope, receive, send)

        task = asyncio.create_task(driver.drive(app, control))
        await control.started.wait()
        return task

    async def test_http_preserves_scope_state_and_streaming_events(self):
        exchange = Exchange()
        state = {"ready": True}

        async def app(scope, receive, send):
            self.assertEqual(scope["http_version"], "2")
            self.assertEqual(scope["headers"], exchange.headers)
            scope["state"]["request_only"] = True
            self.assertEqual((await receive())["body"], b"request")
            await send({"type": "http.response.start", "status": 201})
            await send({"type": "http.response.body", "body": b"one", "more_body": True})
            await send({"type": "http.response.body", "body": b"two"})

        await driver.drive_http(app, exchange, state, {})
        self.assertIsNone(exchange.error)
        self.assertEqual(exchange.events, [(201, []), (b"one", True), (b"two", False)])
        self.assertEqual(state, {"ready": True})

    async def test_lazy_traceback_still_reports_application_errors(self):
        exchange = Exchange()

        async def app(scope, receive, send):
            raise ValueError("example failure")

        await driver.drive_http(app, exchange, {}, {})
        self.assertIn("ValueError: example failure", exchange.error)
        self.assertIn("Traceback", exchange.error)

    async def test_failed_request_does_not_cancel_sibling_or_skip_drain_wakeups(self):
        control = self.control()
        failed, pending = PendingExchange(1), PendingExchange(2)
        cancelled = []
        shutdown = asyncio.Event()

        async def app(scope, receive, send):
            if scope["path"] == "/1":
                raise ValueError("isolated request failure")
            try:
                self.assertEqual((await receive())["body"], b"after stop")
                await send({"type": "http.response.start", "status": 200})
                await send({"type": "http.response.body", "body": b"drained"})
            except asyncio.CancelledError:
                cancelled.append(scope["path"])
                raise

        task = await self.start_driver(control, app, shutdown.set)
        control.push([failed, pending], last=True)
        await failed.completed.wait()
        await pending.waiting.wait()
        await control.stopping.wait()
        self.assertFalse(task.done())
        self.assertFalse(shutdown.is_set())
        self.assertEqual(cancelled, [])
        pending.incoming.append({"type": "http.request", "body": b"after stop"})
        control.push(ready=[pending.id])
        await task
        self.assertIn("isolated request failure", failed.error)
        self.assertEqual(pending.events[-1], (b"drained", False))
        self.assertTrue(shutdown.is_set())
        self.assertEqual(cancelled, [])
        self.assertFalse(asyncio.get_running_loop().remove_reader(control.doorbell()))

    async def test_receive_error_is_isolated_and_channel_waiters_are_released(self):
        control = self.control()
        failed, pending = PendingExchange(1), PendingExchange(2)
        layer = SimpleNamespace(_waiters=None)
        previous = sys.modules.get("gufi.channels")
        sys.modules["gufi.channels"] = layer
        self.addCleanup(
            lambda: sys.modules.pop("gufi.channels", None)
            if previous is None else sys.modules.__setitem__("gufi.channels", previous)
        )

        async def app(scope, receive, send):
            await receive()
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"ok"})

        task = await self.start_driver(control, app)
        control.push([failed, pending])
        await failed.waiting.wait()
        await pending.waiting.wait()
        waiters = layer._waiters
        self.assertEqual(set(waiters), {1, 2})
        failed.incoming.append(ValueError("body read failed"))
        control.push(ready=[failed.id])
        await failed.completed.wait()
        self.assertFalse(pending.completed.is_set())
        pending.incoming.append({"type": "http.request", "body": b"ok"})
        control.push(ready=[pending.id], last=True)
        await task
        self.assertIn("body read failed", failed.error)
        self.assertIsNone(pending.error)
        self.assertEqual(waiters, {})
        self.assertIsNone(layer._waiters)

    async def test_lifespan_decline_leaves_no_message_waiter(self):
        control = self.control()
        before = asyncio.all_tasks()

        async def app(scope, receive, send):
            raise NotImplementedError("no lifespan")

        control.push(last=True)
        await driver.drive(app, control)
        self.assertTrue(control.started.is_set())
        self.assertIn("no lifespan", control.absent[0])
        self.assertEqual(asyncio.all_tasks(), before)
        self.assertEqual(asyncio.current_task().cancelling(), 0)

    async def test_startup_failure_joins_lifespan_cleanup_before_return(self):
        control = self.control()
        cleaned = asyncio.Event()
        before = asyncio.all_tasks()

        async def app(scope, receive, send):
            try:
                await receive()
                await send({"type": "lifespan.startup.failed", "message": "startup failed"})
                await asyncio.Future()
            finally:
                cleaned.set()

        with self.assertRaisesRegex(RuntimeError, "startup failed"):
            await driver.drive(app, control)
        self.assertTrue(cleaned.is_set())
        self.assertFalse(control.started.is_set())
        self.assertEqual(control.failures, ["startup failed"])
        self.assertEqual(asyncio.all_tasks(), before)

    async def test_failed_shutdown_joins_application_and_cleans_reader(self):
        control = self.control()
        cleaned = asyncio.Event()
        before = asyncio.all_tasks()

        async def app(scope, receive, send):
            try:
                await receive()
                await send({"type": "lifespan.startup.complete"})
                await receive()
                await send({"type": "lifespan.shutdown.failed", "message": "shutdown failed"})
                await asyncio.Future()
            finally:
                cleaned.set()

        control.push(last=True)
        with self.assertRaisesRegex(RuntimeError, "shutdown failed"):
            await driver.drive(app, control)
        self.assertTrue(cleaned.is_set())
        self.assertEqual(asyncio.all_tasks(), before)
        self.assertFalse(asyncio.get_running_loop().remove_reader(control.doorbell()))

    async def test_cancellation_is_not_swallowed_and_receive_waiter_is_removed(self):
        exchange = PendingExchange(1)
        waiters = {}

        async def app(scope, receive, send):
            await receive()

        task = asyncio.create_task(driver._drive_exchange(app, exchange, {}, waiters))
        await exchange.waiting.wait()
        self.assertEqual(set(waiters), {1})
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(waiters, {})

    async def test_websocket_failure_does_not_interrupt_sibling_drain(self):
        control = self.control()
        failed, pending = SocketExchange(1), SocketExchange(2)

        async def app(scope, receive, send):
            self.assertEqual((await receive())["type"], "websocket.connect")
            if scope["path"] == "/1":
                raise ValueError("isolated socket failure")
            await send({"type": "websocket.accept"})
            self.assertEqual((await receive())["type"], "websocket.disconnect")
            await send({"type": "websocket.close", "code": 1000})

        task = await self.start_driver(control, app)
        control.push([failed, pending], last=True)
        await failed.completed.wait()
        await pending.waiting.wait()
        self.assertFalse(task.done())
        self.assertFalse(pending.completed.is_set())
        pending.incoming.append({"type": "websocket.disconnect", "code": 1000})
        control.push(ready=[pending.id])
        await task
        self.assertIn("isolated socket failure", failed.error)
        self.assertIsNone(pending.error)
        self.assertEqual(pending.events, [("accepted", None), ("closed", 1000, "")])
        self.assertTrue(pending.completed.is_set())

    async def test_scope_and_failure_reporting_errors_do_not_cancel_other_requests(self):
        control = self.control()
        reports = []
        loop = asyncio.get_running_loop()
        previous = loop.get_exception_handler()
        loop.set_exception_handler(lambda _, context: reports.append(context))
        self.addCleanup(loop.set_exception_handler, previous)

        class BrokenExchange(PendingExchange):
            @property
            def method(self):
                raise ValueError("scope getter failed")

            def fail(self, error):
                raise RuntimeError("failure reporter failed")

        failed, pending = BrokenExchange(1), PendingExchange(2)

        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"ok"})

        task = await self.start_driver(control, app)
        control.push([failed, pending], last=True)
        await task
        self.assertEqual(pending.events[-1], (b"ok", False))
        self.assertIsNone(pending.error)
        self.assertEqual(len(reports), 1)
        self.assertIn("failure reporter failed", str(reports[0]["exception"]))

    async def test_explicit_driver_cancellation_joins_owned_tasks_and_removes_reader(self):
        control = self.control()
        pending = PendingExchange(1)
        cleaned = asyncio.Event()

        async def app(scope, receive, send):
            try:
                await receive()
            finally:
                cleaned.set()

        before = asyncio.all_tasks()
        task = await self.start_driver(control, app)
        control.push([pending], last=True)
        await pending.waiting.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(cleaned.is_set())
        self.assertEqual(asyncio.all_tasks(), before)
        self.assertFalse(asyncio.get_running_loop().remove_reader(control.doorbell()))


if __name__ == "__main__":
    unittest.main()
