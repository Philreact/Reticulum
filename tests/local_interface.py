import select
import threading
import time
import unittest
from collections import deque
from unittest.mock import Mock, patch

from RNS.Interfaces.BackboneInterface import BackboneInterface
from RNS.Interfaces.EventedSocketIO import EventedSocketIO
from RNS.Interfaces.Interface import Interface
from RNS.Interfaces.LocalInterface import LocalClientInterface, LocalServerInterface


class _FakeSocket:
    def __init__(self, fileno):
        self._fileno = fileno

    def fileno(self):
        return self._fileno

    def setblocking(self, value):
        self.blocking = value

    def close(self):
        self.closed = True


class _FakeEpoll:
    def __init__(self):
        self.modifications = []

    def modify(self, fileno, interest):
        self.modifications.append((fileno, interest))


class TestLocalInterfaceIo(unittest.TestCase):
    def test_failed_client_registration_is_rolled_back(self):
        fileno = 42
        interface = type("Interface", (), {"socket": _FakeSocket(fileno)})()

        with patch.object(EventedSocketIO, "ensure_backend"), patch.object(
            EventedSocketIO, "register_in", return_value=False
        ):
            with self.assertRaises(OSError):
                EventedSocketIO.add_client_socket(interface.socket, interface)

        self.assertNotIn(fileno, EventedSocketIO.spawned_interface_filenos)
        self.assertTrue(interface.socket.closed)

    def test_local_server_uses_portable_evented_backend_without_epoll(self):
        with patch.object(Interface, "__init__", return_value=None), patch(
            "RNS.vendor.platformutils.use_epoll", return_value=False
        ), patch.object(EventedSocketIO, "add_listener") as add_listener:
            server = LocalServerInterface(owner=object(), bindport=0)

        self.assertTrue(server.epoll_backend)
        add_listener.assert_called_once_with(server, ("127.0.0.1", 0))

    def test_tx_ready_preserves_read_interest(self):
        fileno = 42
        interface = type("Interface", (), {"socket": _FakeSocket(fileno), "transmit_buffer": b"queued"})()
        fake_epoll = _FakeEpoll()

        with patch.object(EventedSocketIO, "event_backend", "epoll"), patch.object(
            EventedSocketIO, "epoll", fake_epoll
        ), patch.object(
            EventedSocketIO,
            "spawned_interface_filenos",
            {fileno: interface},
        ):
            BackboneInterface.tx_ready(interface)

        self.assertEqual(
            fake_epoll.modifications,
            [(fileno, select.EPOLLIN | select.EPOLLOUT)],
        )

    def test_tx_ready_ignores_stale_fileno_owner(self):
        fileno = 42
        interface = type("Interface", (), {"socket": _FakeSocket(fileno), "transmit_buffer": b"queued"})()
        replacement = type("Interface", (), {"socket": _FakeSocket(fileno), "transmit_buffer": b""})()
        fake_epoll = _FakeEpoll()

        with patch.object(EventedSocketIO, "event_backend", "epoll"), patch.object(
            EventedSocketIO, "epoll", fake_epoll
        ), patch.object(
            EventedSocketIO,
            "spawned_interface_filenos",
            {fileno: replacement},
        ):
            BackboneInterface.tx_ready(interface)

        self.assertEqual(fake_epoll.modifications, [])

    def test_transmit_queue_preserves_concurrent_appends(self):
        interface = LocalClientInterface.__new__(LocalClientInterface)
        interface.transmit_buffer_chunks = deque()
        interface.transmit_buffer_bytes = 0
        interface.transmit_buffer_head_offset = 0
        interface.transmit_buffer_lock = threading.Lock()
        interface.transmit_buffer_first_enqueued_at = 0.0
        interface.transmit_buffer_last_progress_at = time.monotonic()

        interface.queue_transmit_data(b"first")
        pending = interface.peek_transmit_buffer()
        interface.queue_transmit_data(b"second")
        interface.consume_transmit_buffer(len(pending))

        self.assertEqual(interface.peek_transmit_buffer(), b"second")
        self.assertTrue(interface.has_pending_transmit_data())

    def test_chunked_transmit_queue_preserves_partial_head(self):
        interface = LocalClientInterface.__new__(LocalClientInterface)
        interface.transmit_buffer_chunks = deque()
        interface.transmit_buffer_bytes = 0
        interface.transmit_buffer_head_offset = 0
        interface.transmit_buffer_lock = threading.Lock()
        interface.transmit_buffer_first_enqueued_at = 0.0
        interface.transmit_buffer_last_progress_at = time.monotonic()

        interface.queue_transmit_data(b"first")
        interface.queue_transmit_data(b"second")
        interface.consume_transmit_buffer(2)
        self.assertEqual(bytes(interface.peek_transmit_buffer()), b"rst")
        self.assertEqual(interface.transmit_buffer_len(), 9)

        interface.consume_transmit_buffer(3)
        self.assertEqual(bytes(interface.peek_transmit_buffer()), b"second")
        self.assertEqual(interface.transmit_buffer_len(), 6)

    def test_transmit_stall_requires_queued_data_without_progress(self):
        interface = LocalClientInterface.__new__(LocalClientInterface)
        interface.transmit_buffer_chunks = deque([b"queued"])
        interface.transmit_buffer_bytes = len(b"queued")
        interface.transmit_buffer_head_offset = 0
        interface.transmit_buffer_lock = threading.Lock()
        interface.transmit_buffer_first_enqueued_at = time.monotonic() - 12.0
        interface.transmit_buffer_last_progress_at = time.monotonic() - 12.0

        queued_bytes, stalled_for = interface._transmit_stalled_for()

        self.assertEqual(queued_bytes, len(b"queued"))
        self.assertGreaterEqual(stalled_for, 11.0)

    def test_stale_watchdog_generation_cannot_close_replacement_socket(self):
        old_socket = _FakeSocket(42)
        replacement_socket = _FakeSocket(43)
        interface = LocalClientInterface.__new__(LocalClientInterface)
        interface.socket = old_socket
        interface.socket_generation = 1
        interface.online = True
        interface.detached = False
        interface.target_ip = "127.0.0.1"
        interface.target_port = 37428
        interface.socket_path = None
        interface.name = "test"
        interface.transmit_recovery_active = True
        interface.transmit_recovery_lock = threading.Lock()
        interface._transmit_stalled_for = Mock(return_value=(128, 12.0))
        interface.clear_transmit_buffer = Mock()

        def install_replacement(*args, **kwargs):
            interface.socket = replacement_socket
            interface.socket_generation = 2

        # The warning is emitted after the watchdog captures its socket and
        # generation, which provides a deterministic point to model a
        # concurrent reconnect installing a replacement.
        with patch("RNS.log", side_effect=install_replacement):
            interface._recover_stalled_transmit()

        self.assertIs(interface.socket, replacement_socket)
        self.assertEqual(interface.socket_generation, 2)
        interface.clear_transmit_buffer.assert_not_called()

    def test_detach_removes_client_registration_and_queued_data(self):
        fileno = 42
        interface = LocalClientInterface.__new__(LocalClientInterface)
        interface.socket = _FakeSocket(fileno)
        interface.clear_transmit_buffer = Mock()

        with patch.object(
            EventedSocketIO,
            "spawned_interface_filenos",
            {fileno: interface},
        ), patch.object(EventedSocketIO, "deregister_fileno") as deregister:
            EventedSocketIO.detach_client_interface(interface)
            self.assertEqual(EventedSocketIO.spawned_interface_filenos, {})

        deregister.assert_called_once_with(fileno)
        interface.clear_transmit_buffer.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
