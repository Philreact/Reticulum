import unittest

from .hashes import TestSHA256
from .hashes import TestSHA512
from .identity import TestIdentity
from .link import TestLink
from .channel import TestChannel
from .request_receipt import TestRequestReceiptFailure
from .logging import TestLoggingFailureRecovery
from .link_callback_dispatcher import TestLinkPacketCallbackDispatcher
from .discovery import TestCrossPlatformInterfaceDiscovery
from .local_interface import TestLocalInterfaceIo

if __name__ == '__main__':
    unittest.main(verbosity=2)
