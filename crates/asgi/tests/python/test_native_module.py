"""Load shipped adapters against Gufi's native boundary without booting Rust."""

import importlib
import importlib.util
import logging
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load_module(name, source):
    spec = importlib.util.spec_from_file_location(name, ROOT / source)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class NativeModuleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.gufi = ModuleType("gufi")
        self.enterContext(patch.dict(sys.modules, {"gufi": self.gufi}))

    def test_log_handler_uses_gufi_native_channel(self):
        records = []
        self.gufi._log_channel = lambda name: 7 if name == "example" else None
        self.gufi._log = lambda *args, **kwargs: records.append((args, kwargs))
        module = load_module("gufi._log", "src/log.py")
        self.gufi.LogHandler = module.LogHandler
        handler = self.gufi.LogHandler("example")
        self.addCleanup(handler.close)
        record = logging.LogRecord("example.logger", logging.INFO, __file__, 1,
                                   "message %s", ("sent",), None)
        handler.emit(record)
        self.assertEqual(records[0][0], (7, "INFO", "message sent"))
        self.assertEqual(records[0][1]["target"], "example.logger")
        self.assertEqual(type(handler).__module__, "gufi._log")

    async def test_channels_backend_import_uses_gufi_native_core(self):
        calls = []
        self.gufi._channels = SimpleNamespace(
            group_add=lambda *args: calls.append(args),
        )
        exceptions = ModuleType("channels.exceptions")
        exceptions.ChannelFull = type("ChannelFull", (Exception,), {})
        layers = ModuleType("channels.layers")
        layers.BaseChannelLayer = type("BaseChannelLayer", (), {
            "require_valid_group_name": lambda self, name: None,
            "require_valid_channel_name": lambda self, name: None,
        })
        self.enterContext(patch.dict(sys.modules, {
            "msgpack": ModuleType("msgpack"),
            "channels": ModuleType("channels"),
            "channels.exceptions": exceptions,
            "channels.layers": layers,
        }))
        self.gufi.channels = load_module("gufi.channels", "src/django/channels.py")
        backend = importlib.import_module("gufi.channels").OrbitChannelLayer()
        await backend.group_add("example", "specific.example")
        self.assertEqual(calls, [("example", "specific.example")])
        self.assertEqual(type(backend).__module__, "gufi.channels")


if __name__ == "__main__":
    unittest.main()
