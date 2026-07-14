import threading
import unittest
import importlib
from collections import deque
from unittest.mock import Mock, patch

import RNS
from RNS.Interfaces.LocalInterface import LocalClientInterface, LocalSelectorManager

local_interface_module = importlib.import_module("RNS.Interfaces.LocalInterface")


class _FakeSocket:
    def shutdown(self, _how):
        pass

    def close(self):
        pass


class TestLocalInterfaceRecovery(unittest.TestCase):
    def make_interface(self):
        interface = LocalClientInterface.__new__(LocalClientInterface)
        interface.name = "test"
        interface.detached = False
        interface.online = True
        interface.reconnecting = False
        interface.reconnect_lock = threading.Lock()
        interface.is_connected_to_shared_instance = True
        interface.socket = _FakeSocket()
        interface.socket_path = None
        interface.target_ip = "127.0.0.1"
        interface.target_port = 37428
        interface.transmit_buffer = b""
        interface.transmit_buffer_lock = threading.Lock()
        interface.transmit_buffer_chunks = deque([b"abcdef", b"next"])
        interface.transmit_buffer_queued_bytes = 8
        interface.transmit_buffer_head_offset = 2
        interface.qortal_trace_transmit_buffer_first_enqueued_at = 100.0
        interface.qortal_trace_transmit_buffer_last_enqueued_at = 101.0
        interface.transmit_buffer_last_progress_at = 102.0
        interface.transmit_rearm_logged = True
        interface.transmit_recovery_lock = threading.Lock()
        interface.transmit_recovery_active = True
        return interface

    def test_stall_age_uses_last_write_progress(self):
        interface = self.make_interface()
        queued_bytes, stalled_for = interface._transmit_stalled_for(now=107.0)
        self.assertEqual(queued_bytes, 8)
        self.assertEqual(stalled_for, 5.0)

    def test_partial_frame_is_not_replayed_on_new_socket(self):
        interface = self.make_interface()
        discarded = interface._realign_transmit_buffer_for_reconnect()
        self.assertEqual(discarded, 4)
        self.assertEqual(list(interface.transmit_buffer_chunks), [b"next"])
        self.assertEqual(interface.transmit_buffer_queued_bytes, 4)
        self.assertEqual(interface.transmit_buffer_head_offset, 0)

    def test_non_v2_recovery_drops_unaligned_buffer(self):
        interface = self.make_interface()
        interface.transmit_buffer = b"partial-frame"

        with patch.object(local_interface_module, "QORTAL_RNS_LOCAL_IO_V2", False):
            discarded = interface._realign_transmit_buffer_for_reconnect()

        self.assertEqual(discarded, len(b"partial-frame"))
        self.assertEqual(interface.transmit_buffer, b"")
        self.assertEqual(interface.qortal_trace_transmit_buffer_first_enqueued_at, 0.0)

    def test_recovery_keeps_complete_frames_and_reconnects_immediately(self):
        interface = self.make_interface()
        reconnect_calls = []

        def reconnect(immediate=False):
            reconnect_calls.append(immediate)
            interface.online = True

        interface.reconnect = reconnect
        with patch.object(LocalSelectorManager, "_remove_client"), \
             patch.object(RNS.Transport, "shared_connection_disappeared") as disappeared:
            interface._recover_stalled_transmit("test")

        disappeared.assert_called_once_with()
        self.assertEqual(reconnect_calls, [True])
        self.assertEqual(list(interface.transmit_buffer_chunks), [b"next"])
        self.assertEqual(interface.transmit_buffer_queued_bytes, 4)
        self.assertFalse(interface.transmit_recovery_active)

    def test_stale_socket_close_does_not_reconnect_current_socket(self):
        stale_socket = Mock()
        stale_socket.recv.return_value = b""
        interface = Mock()
        interface.HW_MTU = 4096
        interface.socket = object()

        with patch.object(LocalSelectorManager, "_remove_client"):
            LocalSelectorManager._handle_client_read(interface, stale_socket)

        interface.receive.assert_not_called()

    def test_detach_marks_socketless_recovery_as_detached(self):
        interface = self.make_interface()
        interface.socket = None
        interface._stop_local_dispatch = Mock()

        interface.detach()

        self.assertTrue(interface.detached)


if __name__ == "__main__":
    unittest.main()
