import select
import selectors
import socket
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from RNS.Interfaces.EventedSocketIO import EventedSocketIO
from RNS.Interfaces.util.TransmitBuffer import TransmitBuffer


class TestEventedUpstreamIntegration(unittest.TestCase):
    def test_drain_cannot_rearm_a_removed_or_reassigned_socket(self):
        original = SimpleNamespace(
            socket=SimpleNamespace(fileno=lambda: 42), transmit_buffer=b"queued",
            _evented_write_interest_armed=True,
        )
        for mapping in ({}, {42: object()}):
            with self.subTest(mapping=mapping), patch.object(
                EventedSocketIO, "spawned_interface_filenos", mapping
            ), patch.object(EventedSocketIO, "_modify_or_recover_fileno") as modify:
                self.assertFalse(EventedSocketIO._set_client_interest(42, original))
                self.assertFalse(original._evented_write_interest_armed)
                modify.assert_not_called()

    def test_ingress_gate_preserves_transmit_and_resumes_reads(self):
        backends = ["selector"]
        if hasattr(select, "epoll"):
            backends.append("epoll")
        for backend in backends:
            with self.subTest(backend=backend):
                client, peer = socket.socketpair()
                poller = select.epoll() if backend == "epoll" else selectors.SelectSelector()
                with client, peer, poller:
                    client.setblocking(False)
                    peer.settimeout(1.0)
                    interface = SimpleNamespace(
                        socket=client, transmit_buffer=TransmitBuffer(),
                        dp_ingress_gated=False, dp_ingress_bytes=0,
                        detached=False, parent_interface=None, txb=0,
                        HW_MTU=1024, receive=Mock(),
                    )
                    fileno = client.fileno()
                    with patch.object(EventedSocketIO, "event_backend", backend), patch.object(
                        EventedSocketIO, "epoll", poller if backend == "epoll" else None
                    ), patch.object(
                        EventedSocketIO, "selector", poller if backend == "selector" else None
                    ), patch.object(
                        EventedSocketIO, "spawned_interface_filenos", {fileno: interface}
                    ), patch.object(EventedSocketIO, "wake"):
                        EventedSocketIO._register_fileno(fileno, EventedSocketIO._read_mask())
                        self.assertTrue(EventedSocketIO.set_ingress_gated(interface, True))
                        peer.sendall(b"incoming")
                        self.assertEqual(EventedSocketIO._poll(0), [])
                        # An already-delivered read event must also respect the gate.
                        EventedSocketIO._process_event(fileno, EventedSocketIO._read_mask())
                        interface.receive.assert_not_called()

                        interface.transmit_buffer.append(b"outgoing")
                        EventedSocketIO.tx_ready(interface)
                        self.assertFalse(EventedSocketIO._write_client_socket(fileno, interface, client))
                        self.assertEqual(peer.recv(8), b"outgoing")
                        self.assertEqual(interface.txb, 8)
                        self.assertTrue(interface.dp_ingress_gated)
                        self.assertFalse(interface._evented_write_interest_armed)
                        self.assertEqual(EventedSocketIO._poll(0), [])

                        self.assertTrue(EventedSocketIO.set_ingress_gated(interface, False))
                        self.assertTrue(EventedSocketIO._poll(0))
                        self.assertFalse(EventedSocketIO._read_client_socket(fileno, interface, client))
                        interface.receive.assert_called_once_with(b"incoming")
                        self.assertEqual(interface.dp_ingress_bytes, 8)

    def test_failed_ingress_transition_preserves_state_for_retry(self):
        for gated in (False, True):
            for outcome in (False, OSError("selector update failed")):
                with self.subTest(gated=gated, outcome=outcome):
                    interface = SimpleNamespace(socket=SimpleNamespace(fileno=lambda: 42), dp_ingress_gated=gated)
                    with patch.object(EventedSocketIO, "spawned_interface_filenos", {42: interface}), patch.object(
                        EventedSocketIO, "_set_client_interest", side_effect=[outcome, True]
                    ), patch.object(EventedSocketIO, "wake"):
                        if isinstance(outcome, Exception):
                            with self.assertRaises(OSError):
                                EventedSocketIO.set_ingress_gated(interface, not gated)
                        else:
                            self.assertFalse(EventedSocketIO.set_ingress_gated(interface, not gated))
                        self.assertEqual(interface.dp_ingress_gated, gated)
                        self.assertTrue(EventedSocketIO.set_ingress_gated(interface, not gated))
                        self.assertEqual(interface.dp_ingress_gated, not gated)

    def test_coalesced_drain_obeys_event_loop_byte_budget(self):
        buffer = TransmitBuffer()
        buffer.append(b"first")
        buffer.append(b"second")
        sent = []
        client = SimpleNamespace(send=lambda data: sent.append(bytes(data)) or len(data))
        interface = SimpleNamespace(
            transmit_buffer=buffer, txb=0, parent_interface=None,
            detached=False, note_transmit_progress=Mock(),
        )
        with patch("RNS.Interfaces.EventedSocketIO.QORTAL_RNS_EVENTED_IO_WRITE_BUDGET_BYTES", 7), patch.object(
            EventedSocketIO, "_set_client_interest", return_value=True
        ):
            self.assertFalse(EventedSocketIO._write_client_socket(42, interface, client))
            self.assertEqual(b"".join(sent), b"firstse")
            self.assertEqual(len(buffer), 4)
            self.assertEqual(interface.txb, 7)
            interface.note_transmit_progress.assert_called_once_with()
            self.assertFalse(EventedSocketIO._write_client_socket(42, interface, client))
            self.assertEqual(b"".join(sent), b"firstsecond")
            self.assertEqual(len(buffer), 0)
            self.assertEqual(interface.txb, 11)

    def test_coalesced_drain_yields_when_time_budget_expires(self):
        buffer = TransmitBuffer()
        buffer.append(b"queued")
        client = SimpleNamespace(send=Mock())
        interface = SimpleNamespace(transmit_buffer=buffer, txb=0, parent_interface=None, detached=False)
        with patch("RNS.Interfaces.EventedSocketIO.QORTAL_RNS_EVENTED_IO_WRITE_BUDGET_SECONDS", -1), patch.object(
            EventedSocketIO, "_set_client_interest", return_value=True
        ):
            self.assertFalse(EventedSocketIO._write_client_socket(42, interface, client))
            client.send.assert_not_called()
            self.assertEqual(len(buffer), 6)
            self.assertEqual(interface.txb, 0)
