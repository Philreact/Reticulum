import os
import tempfile
import unittest
from unittest.mock import patch

import RNS
from RNS.Discovery import InterfaceDiscovery, discovery_connection_interface_type


class TestCrossPlatformInterfaceDiscovery(unittest.TestCase):
    def test_macos_and_windows_use_tcp_client_for_discovered_servers(self):
        with patch.object(RNS.vendor.platformutils, "is_windows", return_value=False), patch.object(
            RNS.vendor.platformutils, "is_darwin", return_value=True
        ):
            self.assertEqual(discovery_connection_interface_type("BackboneInterface"), "TCPClientInterface")
            self.assertEqual(discovery_connection_interface_type("TCPServerInterface"), "TCPClientInterface")

        with patch.object(RNS.vendor.platformutils, "is_windows", return_value=True), patch.object(
            RNS.vendor.platformutils, "is_darwin", return_value=False
        ):
            self.assertEqual(discovery_connection_interface_type("BackboneInterface"), "TCPClientInterface")
            self.assertEqual(discovery_connection_interface_type("TCPServerInterface"), "TCPClientInterface")

    def test_supported_platform_preserves_backbone_and_connects_tcp_servers_as_clients(self):
        with patch.object(RNS.vendor.platformutils, "is_linux", return_value=True), patch.object(
            RNS.vendor.platformutils, "is_android", return_value=False
        ):
            self.assertEqual(discovery_connection_interface_type("BackboneInterface"), "BackboneInterface")
            self.assertEqual(discovery_connection_interface_type("TCPServerInterface"), "TCPClientInterface")

    def test_tcp_client_discovery_is_accepted_and_persisted(self):
        discovery = object.__new__(InterfaceDiscovery)
        discovery.discovery_callback = None
        discovery.autoconnect = lambda info: None

        with tempfile.TemporaryDirectory() as storagepath:
            discovery.storagepath = storagepath
            info = {
                "name": "Test discovered interface",
                "value": 16,
                "type": "TCPClientInterface",
                "discovery_hash": b"discovery-hash",
                "hops": 1,
                "received": 1.0,
            }

            discovery.interface_discovered(info)

            self.assertEqual(len(os.listdir(storagepath)), 1)


if __name__ == "__main__":
    unittest.main()
