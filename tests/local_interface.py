import select
import selectors
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


class _ConcurrentModifyDetector:
    def __init__(self):
        self.active = 0
        self.maximum_active = 0
        self.lock = threading.Lock()

    def modify(self, fileno, interest):
        with self.lock:
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
        time.sleep(0.01)
        with self.lock:
            self.active -= 1


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

    def test_selector_interest_updates_are_serialized(self):
        selector = _ConcurrentModifyDetector()
        start = threading.Barrier(9)
        results = []

        def update_interest():
            start.wait()
            results.append(EventedSocketIO._modify_or_recover_fileno(42, selectors.EVENT_READ))

        with patch.object(EventedSocketIO, "event_backend", "KqueueSelector"), patch.object(
            EventedSocketIO, "selector", selector
        ):
            threads = [threading.Thread(target=update_interest) for _ in range(8)]
            for thread in threads:
                thread.start()
            start.wait()
            for thread in threads:
                thread.join()

        self.assertEqual(results, [True] * 8)
        self.assertEqual(selector.maximum_active, 1)

    def test_tx_ready_does_not_close_socket_for_selector_state_failure(self):
        fileno = 42
        interface = type("Interface", (), {"socket": _FakeSocket(fileno), "transmit_buffer": b"queued"})()

        with patch.object(
            EventedSocketIO,
            "spawned_interface_filenos",
            {fileno: interface},
        ), patch.object(
            EventedSocketIO, "_modify_or_recover_fileno", return_value=False
        ), patch.object(
            EventedSocketIO, "_close_client_socket"
        ) as close_socket, patch.object(EventedSocketIO, "wake"):
            EventedSocketIO.tx_ready(interface)

        close_socket.assert_not_called()
        self.assertFalse(getattr(interface.socket, "closed", False))

    def test_write_rearm_failure_does_not_close_healthy_socket(self):
        fileno = 42
        client_socket = _FakeSocket(fileno)
        interface = type(
            "Interface",
            (),
            {
                "socket": client_socket,
                "transmit_buffer": b"",
                "detached": False,
                "parent_interface": None,
            },
        )()

        with patch.object(
            EventedSocketIO, "_set_client_interest", return_value=False
        ), patch.object(
            EventedSocketIO, "_close_client_socket"
        ) as close_socket, patch.object(EventedSocketIO, "wake"):
            closed = EventedSocketIO._write_client_socket(fileno, interface, client_socket)

        self.assertFalse(closed)
        close_socket.assert_not_called()
        self.assertFalse(getattr(client_socket, "closed", False))

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
