import threading
import types
import unittest

import RNS
from RNS.Link import LinkCallbacks


def _link():
    link = object.__new__(RNS.Link)
    link.callbacks = LinkCallbacks()
    link.packet_callback_dispatcher = None
    return link


def _receivable_link():
    link = _link()
    interface = object()
    link.status = RNS.Link.ACTIVE
    link.initiator = False
    link.attached_interface = interface
    link.destination = types.SimpleNamespace(proof_strategy=RNS.Destination.PROVE_NONE)
    link.link_id = b"link-id"
    link.last_inbound = 0
    link.last_data = 0
    link.rx = 0
    link.rxbytes = 0
    link.decrypt = lambda data: b"plaintext"
    link._Link__update_phy_stats = lambda packet, query_shared=True, force_update=False: None
    packet = types.SimpleNamespace(
        context=RNS.Packet.NONE,
        data=b"ciphertext",
        receiving_interface=interface,
        packet_type=RNS.Packet.DATA,
        ratchet_id=None,
    )
    return link, packet


class TestLinkPacketCallbackDispatcher(unittest.TestCase):
    def test_dispatcher_receives_callback_data_and_packet(self):
        link, packet = _receivable_link()
        callback_calls = []
        dispatch_calls = []
        callback = lambda data, packet: callback_calls.append((data, packet))
        link.set_packet_callback(callback)
        link.set_packet_callback_dispatcher(lambda cb, data, packet: dispatch_calls.append((cb, data, packet)))

        link.receive(packet)

        self.assertEqual(dispatch_calls, [(callback, b"plaintext", packet)])
        self.assertEqual(callback_calls, [])

    def test_dispatcher_can_be_disabled_and_rejects_invalid_values(self):
        link = _link()
        dispatcher = lambda callback, data, packet: None
        link.set_packet_callback_dispatcher(dispatcher)
        self.assertIs(link.packet_callback_dispatcher, dispatcher)
        link.set_packet_callback_dispatcher(None)
        self.assertIsNone(link.packet_callback_dispatcher)
        with self.assertRaises(TypeError):
            link.set_packet_callback_dispatcher("invalid")

    def test_faulty_dispatcher_falls_back_to_callback_thread(self):
        link, packet = _receivable_link()
        completed = threading.Event()
        link.set_packet_callback(lambda data, received_packet: completed.set())
        link.set_packet_callback_dispatcher(lambda callback, data, received_packet: (_ for _ in ()).throw(RuntimeError("queue failed")))

        link.receive(packet)

        self.assertTrue(completed.wait(timeout=1.0))


if __name__ == "__main__":
    unittest.main()
