import threading
import unittest
from unittest.mock import patch

import RNS


class TestLoggingFailureRecovery(unittest.TestCase):
    def setUp(self):
        self.original_logdest = RNS.logdest
        self.original_logfile = RNS.logfile
        self.original_loglevel = RNS.loglevel
        self.original_override = RNS._always_override_destination
        RNS.loglevel = RNS.LOG_EXTREME
        RNS.logdest = RNS.LOG_FILE
        RNS.logfile = "/unavailable/reticulum-test-log"
        RNS._always_override_destination = False

    def tearDown(self):
        RNS.logdest = self.original_logdest
        RNS.logfile = self.original_logfile
        RNS.loglevel = self.original_loglevel
        RNS._always_override_destination = self.original_override

    def test_file_failure_does_not_recursively_deadlock_logger(self):
        completed = threading.Event()

        def run():
            RNS.log("test message", RNS.LOG_NOTICE)
            completed.set()

        with patch("builtins.open", side_effect=FileNotFoundError("rotated")), \
             patch("builtins.print") as fallback_print:
            thread = threading.Thread(target=run)
            thread.start()
            thread.join(timeout=1.0)

        self.assertTrue(completed.is_set())
        self.assertFalse(thread.is_alive())
        self.assertTrue(RNS._always_override_destination)
        self.assertEqual(fallback_print.call_count, 3)


if __name__ == "__main__":
    unittest.main()
