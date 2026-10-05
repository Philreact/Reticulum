import unittest

from .hashes import TestSHA256
from .hashes import TestSHA512
from .identity import TestIdentity
from .link import TestLink
from .channel import TestChannel
from .hkdf import TestHKDF
from .ifac import TestIFAC
from .coalesced_transmit import TestTransmitBuffer
from .hdlc import TestReceiveBuffer
from .egress import TestHWMLimiter

from .request_receipt import TestRequestReceiptFailure
from .logging import TestLoggingFailureRecovery
from .link_callback_dispatcher import TestLinkPacketCallbackDispatcher
from .discovery import TestCrossPlatformInterfaceDiscovery
from .local_interface import TestLocalInterfaceIo
from .evented_upstream import TestEventedUpstreamIntegration

if __name__ == '__main__': unittest.main(verbosity=2)
