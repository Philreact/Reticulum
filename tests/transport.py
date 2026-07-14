import threading
import time
import unittest
from unittest.mock import Mock, patch

import RNS
from RNS.Transport import (
    IDX_LT_DSTHASH,
    IDX_LT_HOPS,
    IDX_LT_NH_IF,
    IDX_LT_NH_TRID,
    IDX_LT_PROOF_TMO,
    IDX_LT_RCVD_IF,
    IDX_LT_REM_HOPS,
    IDX_LT_TIMESTAMP,
    IDX_LT_VALIDATED,
    Transport,
)


class _LocalParent:
    is_local_shared_instance = True


class _LocalInterface:
    parent_interface = _LocalParent()


class _SharedInterface:
    is_connected_to_shared_instance = True


class TestSharedLinkRouteLiveness(unittest.TestCase):
    def setUp(self):
        self.saved_link_table = Transport.link_table
        self.saved_local_link_owners = Transport.local_link_owners
        self.saved_link_route_last_outbound = Transport.link_route_last_outbound
        self.saved_link_route_candidates = Transport.link_route_candidates
        self.saved_link_route_grace_table = Transport.link_route_grace_table
        self.saved_link_route_last_migrated_at = Transport.link_route_last_migrated_at
        self.saved_link_route_migration_confirming = Transport.link_route_migration_confirming
        self.saved_interfaces = Transport.interfaces
        Transport.link_table = {}
        Transport.local_link_owners = {}
        Transport.link_route_last_outbound = {}
        Transport.link_route_candidates = {}
        Transport.link_route_grace_table = {}
        Transport.link_route_last_migrated_at = {}
        Transport.link_route_migration_confirming = set()
        Transport.interfaces = []

    def tearDown(self):
        Transport.link_table = self.saved_link_table
        Transport.local_link_owners = self.saved_local_link_owners
        Transport.link_route_last_outbound = self.saved_link_route_last_outbound
        Transport.link_route_candidates = self.saved_link_route_candidates
        Transport.link_route_grace_table = self.saved_link_route_grace_table
        Transport.link_route_last_migrated_at = self.saved_link_route_last_migrated_at
        Transport.link_route_migration_confirming = self.saved_link_route_migration_confirming
        Transport.interfaces = self.saved_interfaces

    def make_entry(self, local_interface, network_interface, timestamp=10.0):
        entry = [None] * 9
        entry[IDX_LT_TIMESTAMP] = timestamp
        entry[IDX_LT_NH_TRID] = None
        entry[IDX_LT_NH_IF] = network_interface
        entry[IDX_LT_REM_HOPS] = 1
        entry[IDX_LT_RCVD_IF] = local_interface
        entry[IDX_LT_HOPS] = 1
        entry[IDX_LT_DSTHASH] = b"d" * 16
        entry[IDX_LT_VALIDATED] = True
        entry[IDX_LT_PROOF_TMO] = time.time() + 30
        return entry

    def test_confirmed_owner_activity_refreshes_route(self):
        link_id = b"l" * 16
        local_interface = _LocalInterface()
        entry = self.make_entry(local_interface, object())
        Transport.link_table[link_id] = entry
        Transport.local_link_owners[link_id] = local_interface

        self.assertTrue(Transport._confirm_local_link_activity({"link_id": link_id}, local_interface))
        self.assertGreater(entry[IDX_LT_TIMESTAMP], 10.0)

    def test_non_owner_activity_cannot_refresh_route(self):
        link_id = b"l" * 16
        owner = _LocalInterface()
        other = _LocalInterface()
        entry = self.make_entry(owner, object())
        Transport.link_table[link_id] = entry
        Transport.local_link_owners[link_id] = owner

        self.assertFalse(Transport._confirm_local_link_activity({"link_id": link_id}, other))
        self.assertEqual(entry[IDX_LT_TIMESTAMP], 10.0)

    def test_activity_notifications_are_rate_limited(self):
        shared_interface = _SharedInterface()
        Transport.interfaces = [shared_interface]
        link = Mock()
        link.link_id = b"l" * 16
        link.local_route_activity_notified_at = 0.0
        link.attached_interface = shared_interface
        reticulum = Mock()
        reticulum.is_connected_to_shared_instance = True

        with patch.object(RNS.Reticulum, "get_instance", return_value=reticulum), \
             patch.object(Transport, "transmit") as transmit:
            self.assertTrue(Transport.notify_local_link_activity(link))
            self.assertFalse(Transport.notify_local_link_activity(link))
            self.assertTrue(Transport.notify_local_link_activity(link, force=True))

        self.assertEqual(transmit.call_count, 2)

    def test_unconfirmed_inbound_to_local_owner_does_not_refresh_route(self):
        link_id = b"l" * 16
        local_interface = _LocalInterface()
        entry = self.make_entry(local_interface, object())
        Transport.local_link_owners[link_id] = local_interface
        packet = Mock()
        packet.destination_hash = link_id

        Transport._record_link_route_forward_activity(packet, entry, local_interface, False, now=20.0)

        self.assertEqual(entry[IDX_LT_TIMESTAMP], 10.0)

    def test_transit_inbound_refreshes_route_and_local_outbound_is_separate(self):
        link_id = b"l" * 16
        network_outbound = object()
        entry = self.make_entry(object(), network_outbound)
        packet = Mock()
        packet.destination_hash = link_id

        Transport._record_link_route_forward_activity(packet, entry, network_outbound, False, now=20.0)
        self.assertEqual(entry[IDX_LT_TIMESTAMP], 20.0)

        Transport._record_link_route_forward_activity(packet, entry, network_outbound, True, now=30.0)
        self.assertEqual(entry[IDX_LT_TIMESTAMP], 20.0)
        self.assertEqual(Transport.link_route_last_outbound[link_id], 30.0)

    def test_migration_confirmation_runs_outside_receive_thread(self):
        started = threading.Event()
        release = threading.Event()
        link = Mock()
        link.status = RNS.Link.ACTIVE
        packet = Mock()
        packet.route_migration_candidate = True
        packet.destination_hash = b"l" * 16

        def confirm(_link, _packet):
            started.set()
            release.wait(1.0)
            return False

        with patch.object(Transport, "confirm_link_route_migration", side_effect=confirm) as confirm_mock:
            before = time.monotonic()
            self.assertTrue(Transport.confirm_link_route_migration_async(link, packet))
            self.assertTrue(Transport.confirm_link_route_migration_async(link, packet))
            elapsed = time.monotonic() - before
            self.assertTrue(started.wait(0.25))
            self.assertEqual(confirm_mock.call_count, 1)
            self.assertLess(elapsed, 0.1)
            release.set()
            deadline = time.monotonic() + 0.25
            while packet.destination_hash in Transport.link_route_migration_confirming and time.monotonic() < deadline:
                time.sleep(0.001)
            self.assertNotIn(packet.destination_hash, Transport.link_route_migration_confirming)

    def test_confirmed_migration_updates_only_network_side(self):
        link_id = b"l" * 16
        packet_hash = b"p" * 16
        local_interface = _LocalInterface()
        old_network_interface = object()
        new_network_interface = object()
        entry = self.make_entry(local_interface, old_network_interface)
        Transport.link_table[link_id] = entry
        Transport.local_link_owners[link_id] = local_interface
        Transport.link_route_candidates[(link_id, packet_hash)] = {
            "kind": "daemon_candidate",
            "link_id": link_id,
            "packet_hash": packet_hash,
            "created_at": time.time(),
            "new_interface": new_network_interface,
            "new_hops": 2,
            "local_interface": local_interface,
            "local_side": "received",
            "old_nh_if": old_network_interface,
            "old_rcvd_if": local_interface,
            "old_rem_hops": 1,
            "old_hops": 1,
        }

        self.assertTrue(Transport.confirm_link_route_migration_from_rpc(link_id, packet_hash))
        self.assertIs(entry[IDX_LT_RCVD_IF], local_interface)
        self.assertIs(entry[IDX_LT_NH_IF], new_network_interface)
        self.assertIs(Transport.local_link_owners[link_id], local_interface)


if __name__ == "__main__":
    unittest.main()
