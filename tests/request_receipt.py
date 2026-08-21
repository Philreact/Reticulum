import threading
import unittest

import RNS
from RNS.Link import RequestReceipt


class _Callbacks:
    def __init__(self, failed=None):
        self.response = None
        self.failed = failed
        self.progress = None


class _Link:
    response_resource_concluded = RNS.Link.response_resource_concluded

    def __init__(self):
        self.pending_requests = []


class _Resource:
    def __init__(self, request_id, status):
        self.request_id = request_id
        self.status = status


def _receipt(link, status, failed_callback):
    receipt = object.__new__(RequestReceipt)
    receipt.link = link
    receipt.request_id = b"request-receipt-test"
    receipt.status = status
    receipt.concluded_at = None
    receipt.callbacks = _Callbacks(failed_callback)
    receipt._RequestReceipt__conclusion_lock = threading.Lock()
    link.pending_requests.append(receipt)
    return receipt


class TestRequestReceiptFailure(unittest.TestCase):
    def test_failed_response_resource_concludes_receiving_request(self):
        callbacks = []
        link = _Link()
        receipt = _receipt(link, RequestReceipt.RECEIVING, callbacks.append)
        link.response_resource_concluded(_Resource(receipt.request_id, RNS.Resource.FAILED))
        self.assertEqual(receipt.status, RequestReceipt.FAILED)
        self.assertIsNotNone(receipt.concluded_at)
        self.assertNotIn(receipt, link.pending_requests)
        self.assertEqual(callbacks, [receipt])

    def test_receiving_request_timeout_concludes_once(self):
        callbacks = []
        link = _Link()
        receipt = _receipt(link, RequestReceipt.RECEIVING, callbacks.append)
        receipt.request_timed_out(None)
        receipt.request_timed_out(None)
        self.assertEqual(receipt.status, RequestReceipt.FAILED)
        self.assertNotIn(receipt, link.pending_requests)
        self.assertEqual(callbacks, [receipt])

    def test_sent_request_timeout_is_not_ignored(self):
        callbacks = []
        link = _Link()
        receipt = _receipt(link, RequestReceipt.SENT, callbacks.append)
        receipt.request_timed_out(None)
        self.assertEqual(receipt.status, RequestReceipt.FAILED)
        self.assertEqual(callbacks, [receipt])

    def test_delivered_request_timeout_still_concludes(self):
        callbacks = []
        link = _Link()
        receipt = _receipt(link, RequestReceipt.DELIVERED, callbacks.append)
        receipt.request_timed_out(None)
        self.assertEqual(receipt.status, RequestReceipt.FAILED)
        self.assertEqual(callbacks, [receipt])

    def test_failed_outgoing_request_resource_concludes_once(self):
        callbacks = []
        link = _Link()
        receipt = _receipt(link, RequestReceipt.SENT, callbacks.append)
        resource = _Resource(receipt.request_id, RNS.Resource.FAILED)
        receipt.request_resource_concluded(resource)
        receipt.request_resource_concluded(resource)
        self.assertEqual(receipt.status, RequestReceipt.FAILED)
        self.assertEqual(callbacks, [receipt])

    def test_ready_request_cannot_be_failed(self):
        callbacks = []
        link = _Link()
        receipt = _receipt(link, RequestReceipt.READY, callbacks.append)
        receipt.request_timed_out(None)
        self.assertEqual(receipt.status, RequestReceipt.READY)
        self.assertIn(receipt, link.pending_requests)
        self.assertEqual(callbacks, [])

    def test_concurrent_failures_conclude_once(self):
        callbacks = []
        link = _Link()
        receipt = _receipt(link, RequestReceipt.RECEIVING, callbacks.append)
        start = threading.Barrier(17)

        def fail():
            start.wait()
            receipt.request_timed_out(None)

        threads = [threading.Thread(target=fail) for _ in range(16)]
        for thread in threads:
            thread.start()
        start.wait()
        for thread in threads:
            thread.join()

        self.assertEqual(receipt.status, RequestReceipt.FAILED)
        self.assertNotIn(receipt, link.pending_requests)
        self.assertEqual(callbacks, [receipt])


if __name__ == "__main__":
    unittest.main()
