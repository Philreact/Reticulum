import os
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import RNS
from RNS.Discovery import InterfaceDiscovery, discovery_connection_interface_type


class TestCrossPlatformInterfaceDiscovery(unittest.TestCase):
    def test_autoconnect_keeps_platform_selection_and_upstream_version_policy(self):
        for platform in ("linux", "darwin", "windows"):
            for advertised in ("BackboneInterface", "TCPServerInterface"):
                for version in ("1.5.5", "1.5.1"):
                    with self.subTest(platform=platform, advertised=advertised, version=version):
                        discovery = object.__new__(InterfaceDiscovery)
                        discovery.autoconnect_count = Mock(return_value=0)
                        discovery.endpoint_hash = Mock(return_value=b"endpoint")
                        discovery.interface_exists = Mock(return_value=False)
                        discovery.autoconnect_interface_name = Mock(return_value="Test (2)")
                        discovery.autoconnect_lock = threading.Lock()
                        discovery.monitor_interface = Mock()
                        info = {
                            "type": advertised, "name": "Test", "reachable_on": "example.com",
                            "port": 4242, "config_entry": "", "network_id": b"network",
                            "impl_name": "RNS", "version": version,
                        }
                        instance = Mock()
                        interface = SimpleNamespace()
                        with patch.multiple(
                            RNS.vendor.platformutils,
                            is_windows=Mock(return_value=platform == "windows"),
                            is_darwin=Mock(return_value=platform == "darwin"),
                            is_linux=Mock(return_value=platform == "linux"),
                            is_android=Mock(return_value=False),
                        ), patch.multiple(
                            RNS.Reticulum,
                            should_autoconnect_discovered_interfaces=Mock(return_value=True),
                            max_autoconnected_interfaces=Mock(return_value=3),
                            should_autoconnect_unverified_implementations=Mock(return_value=False),
                            transport_enabled=Mock(return_value=False),
                            autoconnect_interface_mode=Mock(return_value=None),
                            autoconnect_announces_to_internal=Mock(return_value=False),
                            autoconnect_interface_gravity=Mock(return_value=0),
                            get_instance=Mock(return_value=instance),
                        ), patch(
                            "RNS.Interfaces.TCPInterface.TCPClientInterface", return_value=interface
                        ) as tcp, patch(
                            "RNS.Interfaces.BackboneInterface.BackboneClientInterface", return_value=interface
                        ) as backbone, patch("RNS.log"):
                            discovery.autoconnect(info)
                            if version == "1.5.1":
                                tcp.assert_not_called()
                                backbone.assert_not_called()
                                instance._add_interface.assert_not_called()
                            else:
                                expected = backbone if platform == "linux" and advertised == "BackboneInterface" else tcp
                                other = tcp if expected is backbone else backbone
                                expected.assert_called_once_with(
                                    RNS.Transport, {"name": "Test (2)", "target_host": "example.com", "target_port": 4242}
                                )
                                other.assert_not_called()
                                instance._add_interface.assert_called_once()
                                discovery.monitor_interface.assert_called_once_with(interface)
                                self.assertEqual(interface.autoconnect_hash, b"endpoint")

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
