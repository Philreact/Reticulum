# Reticulum License
#
# Copyright (c) 2016-2025 Mark Qvist
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# - The Software shall not be used in any kind of system which includes amongst
#   its functions the ability to purposefully do harm to human beings.
#
# - The Software shall not be used, directly or indirectly, in the creation of
#   an artificial intelligence, machine learning or language model training
#   dataset, including but not limited to any use that contributes to the
#   training or development of such a model or algorithm.
#
# - The above copyright notice and this permission notice shall be included in
#   all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from RNS.Interfaces.Interface import Interface
from RNS.Interfaces.BackboneInterface import BackboneInterface
import socketserver
import threading
import selectors
import socket
import time
import sys
import os
import errno
import RNS
from threading import Lock
from collections import deque

QORTAL_RNS_LOCAL_TRACE = os.environ.get("QORTAL_RNS_LOCAL_TRACE", "0") == "1"
QORTAL_RNS_LOCAL_IO_V2 = os.environ.get("RNS_LOCAL_IO_V2", "1") != "0"
QORTAL_RNS_LOCAL_SELECTOR_IO_V2 = os.environ.get("RNS_LOCAL_SELECTOR_IO_V2", os.environ.get("RNS_LOCAL_IO_V2", "1")) != "0"
QORTAL_RNS_LOCAL_TRACE_GAP_MS = int(os.environ.get("QORTAL_RNS_LOCAL_TRACE_GAP_MS", "320"))
QORTAL_RNS_LOCAL_TRACE_DELAY_MS = int(os.environ.get("QORTAL_RNS_LOCAL_TRACE_DELAY_MS", "80"))
QORTAL_RNS_LOCAL_TRACE_FRAMES = os.environ.get("QORTAL_RNS_LOCAL_TRACE_FRAMES", "0") == "1"
QORTAL_RNS_LOCAL_TRACE_DEST_GAPS = os.environ.get("QORTAL_RNS_LOCAL_TRACE_DEST_GAPS", "0") == "1"
QORTAL_RNS_LOCAL_RX_QUEUE_WARN_BYTES = int(os.environ.get("QORTAL_RNS_LOCAL_RX_QUEUE_WARN_BYTES", str(256*1024)))
QORTAL_RNS_LOCAL_RX_QUEUE_HIGH_BYTES = int(os.environ.get("QORTAL_RNS_LOCAL_RX_QUEUE_HIGH_BYTES", str(512*1024)))
QORTAL_RNS_LOCAL_RX_QUEUE_LOW_BYTES = int(os.environ.get("QORTAL_RNS_LOCAL_RX_QUEUE_LOW_BYTES", str(128*1024)))
QORTAL_RNS_LOCAL_RX_QUEUE_HARD_BYTES = int(os.environ.get("QORTAL_RNS_LOCAL_RX_QUEUE_HARD_BYTES", str(4*1024*1024)))
QORTAL_RNS_LOCAL_RX_BATCH_FRAMES = int(os.environ.get("QORTAL_RNS_LOCAL_RX_BATCH_FRAMES", "64"))
QORTAL_RNS_LOCAL_RX_BATCH_SECONDS = float(os.environ.get("QORTAL_RNS_LOCAL_RX_BATCH_SECONDS", "0.004"))
QORTAL_RNS_LOCAL_RX_QUEUE_WARN_AGE_MS = int(os.environ.get("QORTAL_RNS_LOCAL_RX_QUEUE_WARN_AGE_MS", "100"))
QORTAL_RNS_LOCAL_RX_QUEUE_ERROR_AGE_MS = int(os.environ.get("QORTAL_RNS_LOCAL_RX_QUEUE_ERROR_AGE_MS", "500"))
QORTAL_RNS_LOCAL_RX_INLINE_WARN_MS = int(os.environ.get("QORTAL_RNS_LOCAL_RX_INLINE_WARN_MS", "50"))
QORTAL_RNS_LOCAL_RX_QUEUE_WARN_INTERVAL = 2.0
QORTAL_RNS_LOCAL_TX_QUEUE_WARN_BYTES = 128*1024
QORTAL_RNS_LOCAL_TX_QUEUE_WARN_INTERVAL = 2.0
QORTAL_RNS_LOCAL_TX_QUEUE_WARN_AGE_MS = int(os.environ.get("QORTAL_RNS_LOCAL_TX_QUEUE_WARN_AGE_MS", "100"))
QORTAL_RNS_LOCAL_TX_DRAIN_WARN_MS = int(os.environ.get("QORTAL_RNS_LOCAL_TX_DRAIN_WARN_MS", "25"))
QORTAL_RNS_LOCAL_SELECTOR_POLL_SECONDS = float(os.environ.get("QORTAL_RNS_LOCAL_SELECTOR_POLL_SECONDS", "0.05"))
QORTAL_RNS_LOCAL_TX_DRAIN_MAX_BYTES = 1024*1024
QORTAL_RNS_LOCAL_TX_DRAIN_MAX_SECONDS = 0.006
QORTAL_RNS_LOCAL_RX_READ_MAX_BYTES = 1024*1024
QORTAL_RNS_LOCAL_RX_READ_MAX_SECONDS = 0.004
_LOCAL_RX_CONTINUE = object()

def qortal_local_trace_enabled(interface=None):
    if not QORTAL_RNS_LOCAL_TRACE:
        return False
    if interface == None:
        return True
    if getattr(interface, "qortal_trace_role", None) in ("client", "daemon"):
        return True
    return bool(
        getattr(interface, "is_connected_to_shared_instance", False) or
        getattr(getattr(interface, "parent_interface", None), "is_local_shared_instance", False)
    )

def qortal_local_trace_packet(raw):
    try:
        packet = RNS.Packet(None, raw, create_receipt=False)
        if not packet.unpack():
            return None
        return packet
    except Exception:
        return None

def qortal_local_trace_hash(value):
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()[:16]
    return "n/a"

def qortal_local_trace_role(interface):
    explicit_role = getattr(interface, "qortal_trace_role", None)
    if explicit_role in ("client", "daemon"):
        return explicit_role
    if getattr(interface, "is_connected_to_shared_instance", False):
        return "client"
    if getattr(getattr(interface, "parent_interface", None), "is_local_shared_instance", False):
        return "daemon"
    return "other"

def qortal_local_trace_packet_detail(packet):
    if packet == None:
        return "packet=n/a dest=n/a type=n/a context=n/a"
    return (
        f"packet={qortal_local_trace_hash(getattr(packet, 'packet_hash', None))} "
        f"dest={qortal_local_trace_hash(getattr(packet, 'destination_hash', None))} "
        f"type={getattr(packet, 'packet_type', 'n/a')} "
        f"context={getattr(packet, 'context', 'n/a')}"
    )

def qortal_local_trace_stage_for_role(prefix, interface):
    role = qortal_local_trace_role(interface)
    if role == "daemon":
        return f"local-daemon-{prefix}"
    if role == "client":
        return f"local-client-{prefix}"
    return f"local-{prefix}"

def qortal_local_trace_log(stage, detail):
    if QORTAL_RNS_LOCAL_TRACE:
        RNS.log(f"[qortal-local-trace] stage={stage} {detail}", RNS.LOG_NOTICE)

class HDLC():
    FLAG              = 0x7E
    ESC               = 0x7D
    ESC_MASK          = 0x20

    @staticmethod
    def escape(data):
        data = data.replace(bytes([HDLC.ESC]), bytes([HDLC.ESC, HDLC.ESC^HDLC.ESC_MASK]))
        data = data.replace(bytes([HDLC.FLAG]), bytes([HDLC.ESC, HDLC.FLAG^HDLC.ESC_MASK]))
        return data

class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    def server_bind(self):
        if RNS.vendor.platformutils.is_windows():
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)
        self.server_address = self.socket.getsockname()

class LocalSelectorManager:
    selector = None
    lock = threading.Lock()
    job_active = False
    logged_backend = False
    supported_cache = None
    client_filenos = {}
    listener_filenos = {}

    @staticmethod
    def supported():
        if not QORTAL_RNS_LOCAL_SELECTOR_IO_V2:
            return False
        if RNS.vendor.platformutils.use_epoll():
            return False
        if not RNS.vendor.platformutils.is_darwin():
            return False
        if LocalSelectorManager.supported_cache != None:
            return LocalSelectorManager.supported_cache

        try:
            selector = selectors.DefaultSelector()
            backend_name = selector.__class__.__name__
            selector.close()
            supported = backend_name == "KqueueSelector"
            LocalSelectorManager.supported_cache = supported
            if not LocalSelectorManager.logged_backend:
                LocalSelectorManager.logged_backend = True
                if supported:
                    if QORTAL_RNS_LOCAL_TRACE:
                        RNS.log(f"LocalInterface selector backend={backend_name} default_on=True", RNS.LOG_NOTICE)
                else:
                    RNS.log(f"LocalInterface selector backend={backend_name} default_on=False reason=not_kqueue", RNS.LOG_WARNING)
            return supported

        except Exception as e:
            LocalSelectorManager.supported_cache = False
            if not LocalSelectorManager.logged_backend:
                LocalSelectorManager.logged_backend = True
                RNS.log(f"LocalInterface selector backend unavailable: {e}", RNS.LOG_WARNING)
            return False

    @staticmethod
    def ensure_selector():
        if LocalSelectorManager.selector == None:
            LocalSelectorManager.selector = selectors.DefaultSelector()

    @staticmethod
    def start():
        with LocalSelectorManager.lock:
            if LocalSelectorManager.job_active:
                return
            LocalSelectorManager.ensure_selector()
            LocalSelectorManager.job_active = True
            threading.Thread(target=LocalSelectorManager.__job, daemon=True).start()

    @staticmethod
    def _register_or_modify(sock, events, data, context):
        if sock == None:
            return False
        fileno = sock.fileno()
        if fileno < 0:
            return False
        if events == 0:
            LocalSelectorManager._unregister(sock, context)
            return True
        try:
            with LocalSelectorManager.lock:
                LocalSelectorManager.ensure_selector()
                try:
                    LocalSelectorManager.selector.modify(sock, events, data=data)
                except KeyError:
                    LocalSelectorManager.selector.register(sock, events, data=data)
            return True

        except OSError as e:
            if e.errno in (errno.EBADF, errno.ENOENT):
                RNS.log(f"Ignoring stale selector state update in {context}: {e}", RNS.LOG_DEBUG)
                return False
            RNS.log(f"Error while updating local selector state in {context}: {e}", RNS.LOG_WARNING)
            raise e

    @staticmethod
    def _unregister(sock, context):
        if sock == None:
            return
        try:
            with LocalSelectorManager.lock:
                if LocalSelectorManager.selector != None:
                    try:
                        LocalSelectorManager.selector.unregister(sock)
                    except KeyError:
                        pass
        except OSError as e:
            if e.errno in (errno.EBADF, errno.ENOENT):
                RNS.log(f"Ignoring stale selector unregister in {context}: {e}", RNS.LOG_DEBUG)
            else:
                RNS.log(f"Error while unregistering local selector socket in {context}: {e}", RNS.LOG_DEBUG)
        except Exception as e:
            RNS.log(f"Error while unregistering local selector socket in {context}: {e}", RNS.LOG_DEBUG)

    @staticmethod
    def add_listener(interface, bind_address, socket_type=socket.AF_INET):
        if socket_type == socket.AF_INET:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(bind_address)
        elif socket_type == socket.AF_INET6:
            server_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(bind_address)
        elif socket_type == socket.AF_UNIX:
            server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server_socket.bind(bind_address)
        else:
            raise TypeError(f"Invalid socket type {socket_type} for {interface}")

        server_socket.listen(1)
        server_socket.setblocking(False)
        LocalSelectorManager.listener_filenos[server_socket.fileno()] = (interface, server_socket)
        LocalSelectorManager._register_or_modify(server_socket, selectors.EVENT_READ, ("listener", interface, server_socket), "add_listener")
        LocalSelectorManager.start()

    @staticmethod
    def add_client_socket(client_socket, interface):
        client_socket.setblocking(False)
        LocalSelectorManager.client_filenos[client_socket.fileno()] = interface
        LocalSelectorManager._register_or_modify(client_socket, LocalSelectorManager._events_for_interface(interface), ("client", interface), "add_client_socket")
        LocalSelectorManager.start()

    @staticmethod
    def tx_ready(interface):
        if interface.socket:
            LocalSelectorManager._register_or_modify(interface.socket, LocalSelectorManager._events_for_interface(interface, want_write=True), ("client", interface), "tx_ready")

    @staticmethod
    def set_rx_ready(interface, enabled):
        if interface.socket:
            LocalSelectorManager._register_or_modify(interface.socket, LocalSelectorManager._events_for_interface(interface, want_read=enabled), ("client", interface), "set_rx_ready")

    @staticmethod
    def _events_for_interface(interface, want_read=None, want_write=None):
        if want_read == None:
            want_read = not bool(getattr(interface, "epoll_receive_paused", False))
        if want_write == None:
            want_write = interface.transmit_buffer_len() > 0 if hasattr(interface, "transmit_buffer_len") else False

        events = 0
        if want_read:
            events |= selectors.EVENT_READ
        if want_write:
            events |= selectors.EVENT_WRITE
        return events

    @staticmethod
    def _remove_client(interface, sock=None):
        client_socket = sock if sock != None else getattr(interface, "socket", None)
        fileno = client_socket.fileno() if client_socket != None else -1
        LocalSelectorManager._unregister(client_socket, "remove_client")
        if fileno in LocalSelectorManager.client_filenos:
            try: LocalSelectorManager.client_filenos.pop(fileno)
            except Exception: pass
        try:
            if interface.parent_interface:
                pif = interface.parent_interface
                if hasattr(pif, "spawned_interfaces") and pif.spawned_interfaces != None:
                    while interface in pif.spawned_interfaces: pif.spawned_interfaces.remove(interface)
        except Exception as e:
            RNS.log(f"Error while removing selector interface from parent: {e}", RNS.LOG_ERROR)

    @staticmethod
    def _handle_client_read(interface, client_socket):
        total_read = 0
        read_started = time.monotonic()
        while True:
            if getattr(interface, "epoll_receive_paused", False):
                break
            try:
                received_bytes = client_socket.recv(interface.HW_MTU)
            except BlockingIOError:
                break
            except OSError as e:
                if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    break
                RNS.log(f"Error while reading from {interface}: {e}", RNS.LOG_DEBUG)
                received_bytes = b""
            except Exception as e:
                RNS.log(f"Error while reading from {interface}: {e}", RNS.LOG_DEBUG)
                received_bytes = b""

            if len(received_bytes):
                interface.receive(received_bytes)
                total_read += len(received_bytes)
                if total_read >= QORTAL_RNS_LOCAL_RX_READ_MAX_BYTES:
                    break
                if time.monotonic() - read_started >= QORTAL_RNS_LOCAL_RX_READ_MAX_SECONDS:
                    break
            else:
                LocalSelectorManager._remove_client(interface, client_socket)
                try: client_socket.close()
                except Exception as e: RNS.log(f"Error while closing selector client socket for {interface}: {e}", RNS.LOG_WARNING)
                interface.receive(received_bytes)
                break

    @staticmethod
    def _handle_client_write(interface, client_socket):
        write_failed = False
        total_written = 0
        drain_started = time.monotonic()
        drain_oldest_age_ms = interface.transmit_buffer_oldest_age_ms() if hasattr(interface, "transmit_buffer_oldest_age_ms") else 0.0
        stop_reason = "empty"
        while interface.transmit_buffer_len() > 0:
            try:
                pending = interface.get_transmit_buffer()
                if len(pending) == 0:
                    stop_reason = "empty"
                    break
                written = client_socket.send(pending)
            except BlockingIOError:
                written = 0
                stop_reason = "would_block"
            except OSError as e:
                if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    written = 0
                    stop_reason = "would_block"
                else:
                    written = 0
                    write_failed = True
                    stop_reason = "write_error"
                    if not interface.detached: RNS.log(f"Error while writing to {interface}: {e}", RNS.LOG_DEBUG)
                    LocalSelectorManager._remove_client(interface, client_socket)
                    try: client_socket.close()
                    except Exception as close_error: RNS.log(f"Error while closing selector socket for {interface}: {close_error}", RNS.LOG_WARNING)
                    interface.receive(b"")
            except Exception as e:
                written = 0
                write_failed = True
                stop_reason = "write_exception"
                if not interface.detached: RNS.log(f"Error while writing to {interface}: {e}", RNS.LOG_DEBUG)
                LocalSelectorManager._remove_client(interface, client_socket)
                try: client_socket.close()
                except Exception as close_error: RNS.log(f"Error while closing selector socket for {interface}: {close_error}", RNS.LOG_WARNING)
                interface.receive(b"")

            if write_failed:
                break
            if written <= 0:
                if stop_reason == "empty":
                    stop_reason = "zero_write"
                break

            remaining = interface.discard_transmitted_bytes(written)
            total_written += written
            if total_written >= QORTAL_RNS_LOCAL_TX_DRAIN_MAX_BYTES:
                stop_reason = "byte_budget"
                break
            if time.monotonic() - drain_started >= QORTAL_RNS_LOCAL_TX_DRAIN_MAX_SECONDS:
                stop_reason = "time_budget"
                break
            if remaining == 0:
                stop_reason = "empty"
                break

        if write_failed:
            return

        remaining_after_drain = interface.transmit_buffer_len()
        drain_duration_ms = (time.monotonic() - drain_started) * 1000.0
        if hasattr(interface, "qortal_trace_tx_drain"):
            interface.qortal_trace_tx_drain(
                "tx-drain",
                total_written,
                remaining_after_drain,
                drain_duration_ms,
                stop_reason,
                drain_oldest_age_ms,
            )

        if remaining_after_drain == 0:
            LocalSelectorManager._register_or_modify(client_socket, LocalSelectorManager._events_for_interface(interface, want_write=False), ("client", interface), "tx_drain_empty")

        interface.txb += total_written
        if interface.parent_interface: interface.parent_interface.txb += total_written

    @staticmethod
    def _handle_listener(owner_interface, server_socket):
        while True:
            try:
                client_socket, address = server_socket.accept()
                client_socket.setblocking(False)
                if not owner_interface.incoming_connection(client_socket):
                    try: client_socket.close()
                    except Exception as e: RNS.log(f"Error while closing failed selector incoming connection: {e}", RNS.LOG_WARNING)
            except BlockingIOError:
                break
            except OSError as e:
                if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    break
                RNS.log(f"Accepting selector socket failed for incoming connection: {e}", RNS.LOG_WARNING)
                break
            except Exception as e:
                RNS.log(f"Accepting selector socket failed for incoming connection: {e}", RNS.LOG_WARNING)
                break

    @staticmethod
    def __job():
        while True:
            try:
                with LocalSelectorManager.lock:
                    selector = LocalSelectorManager.selector
                events = selector.select(QORTAL_RNS_LOCAL_SELECTOR_POLL_SECONDS)
                for key, mask in events:
                    data = key.data
                    if data == None:
                        continue
                    if data[0] == "client":
                        interface = data[1]
                        client_socket = key.fileobj
                        if interface.socket == None or client_socket.fileno() != interface.socket.fileno():
                            LocalSelectorManager._unregister(client_socket, "stale_client_event")
                            continue
                        if mask & selectors.EVENT_READ:
                            LocalSelectorManager._handle_client_read(interface, client_socket)
                        if interface.socket != None and client_socket.fileno() >= 0 and client_socket.fileno() == interface.socket.fileno() and mask & selectors.EVENT_WRITE:
                            LocalSelectorManager._handle_client_write(interface, client_socket)

                    elif data[0] == "listener":
                        owner_interface, server_socket = data[1], data[2]
                        if mask & selectors.EVENT_READ:
                            LocalSelectorManager._handle_listener(owner_interface, server_socket)

            except Exception as e:
                RNS.log(f"LocalInterface selector backend error: {e}", RNS.LOG_ERROR)
                RNS.trace_exception(e)

class LocalClientInterface(Interface):
    RECONNECT_WAIT = 8
    AUTOCONFIGURE_MTU = True
    CLIENT_SLEEP_PAUSE_TIMEOUT = 12

    def __init__(self, owner, name, target_port = None, connected_socket=None, socket_path=None):
        super().__init__()

        self.epoll_backend    = False
        self.selector_backend = False
        self.HW_MTU           = 262144
        self.online           = False

        if socket_path != None and RNS.Reticulum.get_instance().use_af_unix: self.socket_path = f"\0rns/{socket_path}"
        else: self.socket_path = None

        self.IN               = True
        self.OUT              = False
        self.socket           = None
        self.parent_interface = None
        self.reconnecting     = False
        self.never_connected  = True
        self.detached         = False
        self.name             = name
        self.mode             = RNS.Interfaces.Interface.Interface.MODE_FULL
        self.frame_buffer     = bytearray()
        self.transmit_buffer  = b""
        self.transmit_buffer_lock = Lock()
        self.transmit_buffer_chunks = deque()
        self.transmit_buffer_queued_bytes = 0
        self.transmit_buffer_head_offset = 0
        self.qortal_trace_role = None
        self.qortal_trace_last_recv_at = 0.0
        self.qortal_trace_last_frame_at_by_destination = {}
        self.qortal_trace_transmit_buffer_first_enqueued_at = 0.0
        self.qortal_trace_transmit_buffer_last_enqueued_at = 0.0
        self.qortal_trace_transmit_buffer_last_warned_at = 0.0
        self.epoll_receive_queue = deque()
        self.epoll_receive_queue_bytes = 0
        self.epoll_receive_queue_condition = threading.Condition()
        self.epoll_receive_worker_started = False
        self.epoll_receive_processing = False
        self.epoll_receive_queue_last_warned_at = 0.0
        self.epoll_receive_paused = False
        self.epoll_receive_hard_warned_at = 0.0
        self.epoll_receive_continue_queued = False

        if RNS.vendor.platformutils.use_epoll():
            self.epoll_backend = True
        elif LocalSelectorManager.supported():
            self.selector_backend = True

        self.pause_on_client_sleep = False

        if connected_socket != None:
            self.receives    = True
            self.target_ip   = None
            self.target_port = None
            self.socket      = connected_socket

            if self.socket.family == socket.AF_INET:
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            self.is_connected_to_shared_instance = False

            if RNS.vendor.platformutils.is_android():
                self.pause_on_client_sleep = True
                self.pause_timeout = time.time() + self.CLIENT_SLEEP_PAUSE_TIMEOUT

        elif self.socket_path != None:
            self.receives    = True
            self.target_ip   = None
            self.target_port = None
            self.connect()

        elif target_port != None:
            self.receives    = True
            self.target_ip   = "127.0.0.1"
            self.target_port = target_port
            self.connect()

        self.owner   = owner
        self.bitrate = 1_000_000_000
        self.online  = True
        self.writing = False

        self._force_bitrate = False

        self.announce_rate_target  = None
        self.announce_rate_grace   = None
        self.announce_rate_penalty = None

        if connected_socket == None:
            if not self.epoll_backend and not self.selector_backend:
                thread = threading.Thread(target=self.read_loop)
                thread.daemon = True
                thread.start()

    def should_ingress_limit(self):
        return False

    def connect(self):
        if self.socket_path != None:
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.socket.connect(self.socket_path)

        else:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.socket.connect((self.target_ip, self.target_port))

        self.online = True
        self.is_connected_to_shared_instance = True
        self.qortal_trace_role = "client"
        self.never_connected = False

        if RNS.vendor.platformutils.is_android(): self.phy_keepalive = True
        if self.epoll_backend: BackboneInterface.add_client_socket(self.socket, self)
        elif self.selector_backend: LocalSelectorManager.add_client_socket(self.socket, self)

        return True


    def reconnect(self):
        if self.is_connected_to_shared_instance:
            if not self.reconnecting:
                self.reconnecting = True
                attempts = 0

                while not self.online:
                    time.sleep(LocalClientInterface.RECONNECT_WAIT)
                    attempts += 1

                    try:
                        self.connect()

                    except Exception as e:
                        RNS.log("Connection attempt for "+str(self)+" failed: "+str(e), RNS.LOG_DEBUG)

                if not self.never_connected:
                    RNS.log("Reconnected socket for "+str(self)+".", RNS.LOG_INFO)

                self.reconnecting = False
                if self.selector_backend:
                    LocalSelectorManager.add_client_socket(self.socket, self)
                elif not self.epoll_backend:
                    thread = threading.Thread(target=self.read_loop)
                    thread.daemon = True
                    thread.start()

                def job():
                    time.sleep(LocalClientInterface.RECONNECT_WAIT+2)
                    RNS.Transport.shared_connection_reappeared()
                threading.Thread(target=job, daemon=True).start()

        else:
            RNS.log("Attempt to reconnect on a non-initiator shared local interface. This should not happen.", RNS.LOG_ERROR)
            raise IOError("Attempt to reconnect on a non-initiator local interface")


    def send_keepalive(self):
        if self.online:
            RNS.log(f"Sending keepalive on {self}", RNS.LOG_DEBUG) # TODO: Remove
            try:
                if self.epoll_backend:
                    self.append_transmit_frame(bytes([HDLC.FLAG])+bytes([HDLC.FLAG]))
                    BackboneInterface.tx_ready(self)

                elif self.selector_backend:
                    self.append_transmit_frame(bytes([HDLC.FLAG])+bytes([HDLC.FLAG]))
                    LocalSelectorManager.tx_ready(self)

                else:
                    self.writing = True
                    frame = bytes([HDLC.FLAG])+bytes([HDLC.FLAG])
                    self.socket.sendall(frame)
                    self.writing = False

            except Exception as e: RNS.log(f"Exception occurred while sending keepalive on {self}: {e}", RNS.LOG_ERROR)

    def process_incoming(self, data):
        self.rxb += len(data)
        if self.parent_interface != None: self.parent_interface.rxb += len(data)

        try:
            if hasattr(self.owner, "handle_local_control_frame") and self.owner.handle_local_control_frame(data, self):
                return
            self.owner.inbound(data, self)
        except Exception as e:
            RNS.log(f"An error occurred in the processing of an incoming frame for {self}: {e}", RNS.LOG_ERROR)
            RNS.trace_exception(e)

    def append_transmit_frame(self, frame):
        now = time.monotonic()
        with self.transmit_buffer_lock:
            queued_before = self.transmit_buffer_queued_bytes if QORTAL_RNS_LOCAL_IO_V2 else len(self.transmit_buffer)
            if QORTAL_RNS_LOCAL_IO_V2:
                self.transmit_buffer_chunks.append(frame)
                self.transmit_buffer_queued_bytes += len(frame)
                queued_after = self.transmit_buffer_queued_bytes
            else:
                self.transmit_buffer += frame
                queued_after = len(self.transmit_buffer)
            if queued_before == 0:
                self.qortal_trace_transmit_buffer_first_enqueued_at = now
            self.qortal_trace_transmit_buffer_last_enqueued_at = now
            queued_chunks = len(self.transmit_buffer_chunks) if QORTAL_RNS_LOCAL_IO_V2 else (1 if len(self.transmit_buffer) > 0 else 0)

        if queued_after >= QORTAL_RNS_LOCAL_TX_QUEUE_WARN_BYTES:
            if now - self.qortal_trace_transmit_buffer_last_warned_at >= QORTAL_RNS_LOCAL_TX_QUEUE_WARN_INTERVAL:
                self.qortal_trace_transmit_buffer_last_warned_at = now
                RNS.log(
                    f"LocalInterface transmit queue high for {self}: "
                    f"queued_bytes={queued_after} queued_chunks={queued_chunks}",
                    RNS.LOG_WARNING
                )

        return queued_before, queued_after

    def get_transmit_buffer(self):
        with self.transmit_buffer_lock:
            if QORTAL_RNS_LOCAL_IO_V2:
                if len(self.transmit_buffer_chunks) == 0:
                    return b""
                head = self.transmit_buffer_chunks[0]
                if self.transmit_buffer_head_offset > 0:
                    return memoryview(head)[self.transmit_buffer_head_offset:]
                return memoryview(head)
            return self.transmit_buffer

    def discard_transmitted_bytes(self, byte_count):
        with self.transmit_buffer_lock:
            if QORTAL_RNS_LOCAL_IO_V2:
                remaining_to_discard = byte_count
                while remaining_to_discard > 0 and len(self.transmit_buffer_chunks) > 0:
                    head = self.transmit_buffer_chunks[0]
                    head_remaining = len(head) - self.transmit_buffer_head_offset
                    if remaining_to_discard < head_remaining:
                        self.transmit_buffer_head_offset += remaining_to_discard
                        self.transmit_buffer_queued_bytes -= remaining_to_discard
                        remaining_to_discard = 0
                    else:
                        remaining_to_discard -= head_remaining
                        self.transmit_buffer_queued_bytes -= head_remaining
                        self.transmit_buffer_chunks.popleft()
                        self.transmit_buffer_head_offset = 0
                remaining = self.transmit_buffer_queued_bytes
            else:
                if byte_count > 0:
                    self.transmit_buffer = self.transmit_buffer[byte_count:]
                remaining = len(self.transmit_buffer)
            if remaining == 0:
                self.qortal_trace_transmit_buffer_first_enqueued_at = 0.0
                self.qortal_trace_transmit_buffer_last_enqueued_at = 0.0
            return remaining

    def transmit_buffer_len(self):
        with self.transmit_buffer_lock:
            if QORTAL_RNS_LOCAL_IO_V2:
                return self.transmit_buffer_queued_bytes
            return len(self.transmit_buffer)

    def transmit_buffer_chunk_count(self):
        with self.transmit_buffer_lock:
            if QORTAL_RNS_LOCAL_IO_V2:
                return len(self.transmit_buffer_chunks)
            return 1 if len(self.transmit_buffer) > 0 else 0

    def transmit_buffer_oldest_age_ms(self):
        with self.transmit_buffer_lock:
            first_enqueued_at = self.qortal_trace_transmit_buffer_first_enqueued_at
        if first_enqueued_at <= 0:
            return 0.0
        return (time.monotonic() - first_enqueued_at) * 1000.0

    def qortal_trace_tx_drain(self, stage, written, remaining, duration_ms, stop_reason, oldest_age_ms=None):
        if not qortal_local_trace_enabled(self):
            return
        age_ms = oldest_age_ms if oldest_age_ms != None else self.transmit_buffer_oldest_age_ms()
        if (
            not QORTAL_RNS_LOCAL_TRACE_FRAMES
            and age_ms < QORTAL_RNS_LOCAL_TX_QUEUE_WARN_AGE_MS
            and duration_ms < QORTAL_RNS_LOCAL_TX_DRAIN_WARN_MS
            and remaining == 0
        ):
            return

        qortal_local_trace_log(
            qortal_local_trace_stage_for_role(stage, self),
            f"role={qortal_local_trace_role(self)} interface={self} "
            f"fileno={self.socket.fileno() if self.socket else 'n/a'} "
            f"written={written} remaining={remaining} queued_chunks={self.transmit_buffer_chunk_count()} "
            f"oldest_age_ms={age_ms:.3f} duration_ms={duration_ms:.3f} "
            f"stop={stop_reason}"
        )

    def process_outgoing(self, data):
        if self.pause_on_client_sleep and time.time() > self.pause_timeout:
            RNS.log(f"TX paused for LocalInterface client, dropping outbound packet", RNS.LOG_DEBUG) # TODO: Remove
            return

        if self.online:
            try:
                if self.epoll_backend or self.selector_backend:
                    qortal_enqueue_start = time.monotonic() if qortal_local_trace_enabled(self) else 0.0
                    qortal_raw_packet = qortal_local_trace_packet(data) if qortal_enqueue_start else None
                    frame = bytes([HDLC.FLAG])+HDLC.escape(data)+bytes([HDLC.FLAG])
                    qortal_queued_before, qortal_queued_after = self.append_transmit_frame(frame)
                    if qortal_enqueue_start:
                        if QORTAL_RNS_LOCAL_TRACE_FRAMES:
                            qortal_local_trace_log(
                                qortal_local_trace_stage_for_role("enqueue-frame", self),
                                f"role={qortal_local_trace_role(self)} interface={self} "
                                f"fileno={self.socket.fileno() if self.socket else 'n/a'} "
                                f"queued_before={qortal_queued_before} queued_after={qortal_queued_after} "
                                f"frame_len={len(frame)} tx_ready=yes backend={'epoll' if self.epoll_backend else 'selector'} "
                                f"{qortal_local_trace_packet_detail(qortal_raw_packet)}"
                            )
                    if self.epoll_backend:
                        BackboneInterface.tx_ready(self)
                    else:
                        LocalSelectorManager.tx_ready(self)

                else:
                    self.writing = True

                    if self._force_bitrate:
                        if not hasattr(self, "send_lock"):
                            self.send_lock = Lock()

                        with self.send_lock:
                            # RNS.log(f"Simulating latency of {RNS.prettytime(s)} for {len(data)} bytes", RNS.LOG_EXTREME)
                            s = len(data) / self.bitrate * 8
                            time.sleep(s)

                    raw_packet = qortal_local_trace_packet(data) if qortal_local_trace_enabled(self) else None
                    data = bytes([HDLC.FLAG])+HDLC.escape(data)+bytes([HDLC.FLAG])
                    qortal_send_start = time.monotonic() if qortal_local_trace_enabled(self) else 0.0
                    self.socket.sendall(data)
                    if qortal_send_start:
                        qortal_send_ms = (time.monotonic() - qortal_send_start) * 1000.0
                        if QORTAL_RNS_LOCAL_TRACE_FRAMES:
                            qortal_local_trace_log(
                                qortal_local_trace_stage_for_role("send-frame", self),
                                f"role={qortal_local_trace_role(self)} interface={self} "
                                f"duration_ms={qortal_send_ms:.3f} len={len(data)} "
                                f"{qortal_local_trace_packet_detail(raw_packet)}"
                            )
                        if qortal_send_ms >= QORTAL_RNS_LOCAL_TRACE_DELAY_MS:
                            qortal_local_trace_log(
                                "local-sendall-delay",
                                f"role={qortal_local_trace_role(self)} interface={self} "
                                f"duration_ms={qortal_send_ms:.3f} len={len(data)} "
                                f"{qortal_local_trace_packet_detail(raw_packet)}"
                            )
                    self.writing = False
                    self.txb += len(data)
                    if hasattr(self, "parent_interface") and self.parent_interface != None:
                        self.parent_interface.txb += len(data)

            except Exception as e:
                RNS.log("Exception occurred while transmitting via "+str(self)+", tearing down interface", RNS.LOG_ERROR)
                RNS.log("The contained exception was: "+str(e), RNS.LOG_ERROR)
                RNS.trace_exception(e)
                self.teardown()

    def _frame_buffer_has_complete_frame(self):
        frame_start = self.frame_buffer.find(HDLC.FLAG)
        if frame_start == -1:
            return False
        return self.frame_buffer.find(HDLC.FLAG, frame_start+1) != -1

    def handle_hdlc(self, data_in, max_frames=None, max_seconds=None):
        started_at = time.monotonic()
        processed_frames = 0
        if len(data_in) > 0:
            self.frame_buffer.extend(data_in)
        flags_remaining = True
        while flags_remaining:
            if max_frames != None and processed_frames >= max_frames:
                return processed_frames, self._frame_buffer_has_complete_frame()
            if max_seconds != None and processed_frames > 0 and time.monotonic() - started_at >= max_seconds:
                return processed_frames, self._frame_buffer_has_complete_frame()

            frame_start = self.frame_buffer.find(HDLC.FLAG)
            if frame_start != -1:
                frame_end = self.frame_buffer.find(HDLC.FLAG, frame_start+1)
                if frame_end != -1:
                    frame = bytes(self.frame_buffer[frame_start+1:frame_end])
                    frame = frame.replace(bytes([HDLC.ESC, HDLC.FLAG ^ HDLC.ESC_MASK]), bytes([HDLC.FLAG]))
                    frame = frame.replace(bytes([HDLC.ESC, HDLC.ESC  ^ HDLC.ESC_MASK]), bytes([HDLC.ESC]))
                    if len(frame) > RNS.Reticulum.HEADER_MINSIZE:
                        if qortal_local_trace_enabled(self):
                            packet = qortal_local_trace_packet(frame)
                            if packet != None:
                                now = time.monotonic()
                                destination_hash = getattr(packet, "destination_hash", None)
                                destination_hex = bytes(destination_hash).hex() if isinstance(destination_hash, (bytes, bytearray)) else ""
                                previous = self.qortal_trace_last_frame_at_by_destination.get(destination_hex, 0.0)
                                gap_ms = (now - previous) * 1000.0 if previous else 0.0
                                self.qortal_trace_last_frame_at_by_destination[destination_hex] = now
                                if QORTAL_RNS_LOCAL_TRACE_FRAMES:
                                    qortal_local_trace_log(
                                        qortal_local_trace_stage_for_role("read-frame", self),
                                        f"role={qortal_local_trace_role(self)} interface={self} "
                                        f"dest_gap_ms={gap_ms:.3f} len={len(frame)} "
                                        f"{qortal_local_trace_packet_detail(packet)}"
                                    )
                                if QORTAL_RNS_LOCAL_TRACE_DEST_GAPS and gap_ms >= QORTAL_RNS_LOCAL_TRACE_GAP_MS:
                                    qortal_local_trace_log(
                                        "local-hdlc-frame-gap",
                                        f"role={qortal_local_trace_role(self)} interface={self} "
                                        f"dest_gap_ms={gap_ms:.3f} len={len(frame)} "
                                        f"{qortal_local_trace_packet_detail(packet)}"
                                    )
                        self.process_incoming(frame)
                        processed_frames += 1
                    del self.frame_buffer[:frame_end]

                else: flags_remaining = False

            else: flags_remaining = False
        return processed_frames, False

    def _ensure_epoll_receive_worker(self):
        with self.epoll_receive_queue_condition:
            self._ensure_epoll_receive_worker_locked()

    def _ensure_epoll_receive_worker_locked(self):
        if not self.epoll_receive_worker_started:
            self.epoll_receive_worker_started = True
            thread = threading.Thread(target=self._epoll_receive_worker, daemon=True)
            thread.start()

    def _warn_epoll_receive_queue_locked(self, now):
        if len(self.epoll_receive_queue) == 0:
            return

        oldest_enqueued_at, _ = self.epoll_receive_queue[0]
        oldest_age_ms = (now - oldest_enqueued_at) * 1000.0
        queued_bytes = self.epoll_receive_queue_bytes
        queued_chunks = len(self.epoll_receive_queue)
        if oldest_age_ms < QORTAL_RNS_LOCAL_RX_QUEUE_WARN_AGE_MS and queued_bytes < QORTAL_RNS_LOCAL_RX_QUEUE_WARN_BYTES:
            return
        if now - self.epoll_receive_queue_last_warned_at < QORTAL_RNS_LOCAL_RX_QUEUE_WARN_INTERVAL:
            return

        self.epoll_receive_queue_last_warned_at = now
        level = RNS.LOG_ERROR if oldest_age_ms >= QORTAL_RNS_LOCAL_RX_QUEUE_ERROR_AGE_MS else RNS.LOG_WARNING
        RNS.log(
            f"LocalInterface receive queue delayed for {self}: "
            f"oldest_age_ms={oldest_age_ms:.1f} queued_bytes={queued_bytes} queued_chunks={queued_chunks}",
            level
        )

    def _maybe_update_epoll_receive_interest_locked(self):
        if not (self.epoll_backend or self.selector_backend) or not QORTAL_RNS_LOCAL_IO_V2:
            return
        if self.socket == None:
            return

        if not self.epoll_receive_paused and self.epoll_receive_queue_bytes >= QORTAL_RNS_LOCAL_RX_QUEUE_HIGH_BYTES:
            try:
                self.epoll_receive_paused = True
                if self.epoll_backend:
                    BackboneInterface.set_rx_ready(self, False)
                else:
                    LocalSelectorManager.set_rx_ready(self, False)
                RNS.log(
                    f"LocalInterface receive queue paused for {self}: "
                    f"queued_bytes={self.epoll_receive_queue_bytes} queued_chunks={len(self.epoll_receive_queue)}",
                    RNS.LOG_WARNING
                )
            except Exception as e:
                self.epoll_receive_paused = False
                RNS.log(f"Error while pausing RX readiness for {self}: {e}", RNS.LOG_WARNING)

        if self.epoll_receive_paused and self.epoll_receive_queue_bytes <= QORTAL_RNS_LOCAL_RX_QUEUE_LOW_BYTES:
            try:
                self.epoll_receive_paused = False
                if self.epoll_backend:
                    BackboneInterface.set_rx_ready(self, True)
                else:
                    LocalSelectorManager.set_rx_ready(self, True)
                RNS.log(
                    f"LocalInterface receive queue resumed for {self}: "
                    f"queued_bytes={self.epoll_receive_queue_bytes} queued_chunks={len(self.epoll_receive_queue)}",
                    RNS.LOG_NOTICE
                )
            except Exception as e:
                self.epoll_receive_paused = True
                RNS.log(f"Error while resuming RX readiness for {self}: {e}", RNS.LOG_WARNING)

    def _enqueue_epoll_receive_locked(self, data_in, now):
        self._ensure_epoll_receive_worker_locked()
        self.epoll_receive_queue.append((now, data_in))
        if data_in is not _LOCAL_RX_CONTINUE:
            self.epoll_receive_queue_bytes += len(data_in)
        if self.epoll_receive_queue_bytes >= QORTAL_RNS_LOCAL_RX_QUEUE_HARD_BYTES:
            if now - self.epoll_receive_hard_warned_at >= QORTAL_RNS_LOCAL_RX_QUEUE_WARN_INTERVAL:
                self.epoll_receive_hard_warned_at = now
                RNS.log(
                    f"LocalInterface receive queue over hard limit for {self}: "
                    f"queued_bytes={self.epoll_receive_queue_bytes} queued_chunks={len(self.epoll_receive_queue)}",
                    RNS.LOG_ERROR
                )
        self._warn_epoll_receive_queue_locked(now)
        self._maybe_update_epoll_receive_interest_locked()
        self.epoll_receive_queue_condition.notify()

    def _enqueue_epoll_receive(self, data_in):
        now = time.monotonic()
        with self.epoll_receive_queue_condition:
            self._enqueue_epoll_receive_locked(data_in, now)

    def _finish_epoll_receive_processing(self):
        with self.epoll_receive_queue_condition:
            self.epoll_receive_processing = False
            self._maybe_update_epoll_receive_interest_locked()
            if len(self.epoll_receive_queue) > 0:
                self._ensure_epoll_receive_worker_locked()
                self.epoll_receive_queue_condition.notify()

    def _process_epoll_receive_chunk(self, data_in, queued_at=None):
        started_at = time.monotonic()
        is_continue = data_in is _LOCAL_RX_CONTINUE
        if queued_at != None and qortal_local_trace_enabled(self) and not is_continue:
            queue_age_ms = (started_at - queued_at) * 1000.0
            if QORTAL_RNS_LOCAL_TRACE_FRAMES or queue_age_ms >= QORTAL_RNS_LOCAL_RX_QUEUE_WARN_AGE_MS:
                qortal_local_trace_log(
                    "local-rx-queue-drain",
                    f"role={qortal_local_trace_role(self)} interface={self} "
                    f"queue_age_ms={queue_age_ms:.3f} len={len(data_in)}"
                )

        try:
            if QORTAL_RNS_LOCAL_IO_V2:
                self._receive_inline_batched(data_in)
            else:
                self._receive_inline(data_in)
        finally:
            if qortal_local_trace_enabled(self):
                duration_ms = (time.monotonic() - started_at) * 1000.0
                if duration_ms >= QORTAL_RNS_LOCAL_RX_INLINE_WARN_MS:
                    qortal_local_trace_log(
                        "local-rx-process-delay",
                        f"role={qortal_local_trace_role(self)} interface={self} "
                        f"duration_ms={duration_ms:.3f} len={0 if is_continue else len(data_in)}"
                    )
            self._finish_epoll_receive_processing()

    def _receive_inline_batched(self, data_in):
        is_continue = data_in is _LOCAL_RX_CONTINUE
        if not is_continue and len(data_in) == 0:
            self._receive_inline(data_in)
            return

        try:
            processed_frames, more_frames = self.handle_hdlc(
                b"" if is_continue else data_in,
                max_frames=QORTAL_RNS_LOCAL_RX_BATCH_FRAMES,
                max_seconds=QORTAL_RNS_LOCAL_RX_BATCH_SECONDS,
            )
            if more_frames:
                if qortal_local_trace_enabled(self):
                    qortal_local_trace_log(
                        "local-rx-batch-yield",
                        f"role={qortal_local_trace_role(self)} interface={self} "
                        f"frames={processed_frames} frame_buffer={len(self.frame_buffer)}"
                    )
                with self.epoll_receive_queue_condition:
                    if not self.epoll_receive_continue_queued:
                        self.epoll_receive_continue_queued = True
                        self.epoll_receive_queue.append((time.monotonic(), _LOCAL_RX_CONTINUE))
                        self.epoll_receive_queue_condition.notify()

        except Exception as e:
            self.online = False
            RNS.log("An interface error occurred, the contained exception was: "+str(e), RNS.LOG_ERROR)
            RNS.log("Tearing down "+str(self), RNS.LOG_ERROR)
            self.teardown()

    def _epoll_receive_worker(self):
        while True:
            with self.epoll_receive_queue_condition:
                while len(self.epoll_receive_queue) == 0 or self.epoll_receive_processing:
                    self.epoll_receive_queue_condition.wait()

                self.epoll_receive_processing = True
                queued_at, data_in = self.epoll_receive_queue.popleft()
                if data_in is _LOCAL_RX_CONTINUE:
                    self.epoll_receive_continue_queued = False
                if data_in is not _LOCAL_RX_CONTINUE:
                    self.epoll_receive_queue_bytes -= len(data_in)

            self._process_epoll_receive_chunk(data_in, queued_at)

    def _receive_inline(self, data_in):
        try:
            if len(data_in) > 0: self.handle_hdlc(data_in)
            else:
                self.online = False
                if self.is_connected_to_shared_instance and not self.detached:
                    RNS.log("Socket for "+str(self)+" was closed, attempting to reconnect...", RNS.LOG_WARNING)
                    RNS.Transport.shared_connection_disappeared()
                    # TODO: Potentially run this in a thread, but since if we get here,
                    # there's no other connectivity left to block anyway, it might be
                    # unnecessary.
                    self.reconnect()
                else:
                    self.teardown(nowarning=True)

        except Exception as e:
            self.online = False
            RNS.log("An interface error occurred, the contained exception was: "+str(e), RNS.LOG_ERROR)
            RNS.log("Tearing down "+str(self), RNS.LOG_ERROR)
            self.teardown()

        if self.pause_on_client_sleep: self.pause_timeout = time.time() + self.CLIENT_SLEEP_PAUSE_TIMEOUT

    def receive(self, data_in):
        if self.epoll_backend or self.selector_backend:
            now = time.monotonic()
            with self.epoll_receive_queue_condition:
                if QORTAL_RNS_LOCAL_IO_V2:
                    self._enqueue_epoll_receive_locked(data_in, now)
                    return
                if self.epoll_receive_processing or len(self.epoll_receive_queue) > 0:
                    self._enqueue_epoll_receive_locked(data_in, now)
                    return
                self.epoll_receive_processing = True
            self._process_epoll_receive_chunk(data_in)
        else:
            self._receive_inline(data_in)

    def read_loop(self):
        try:
            self.frame_buffer = bytearray()
            data_in = b""
            while True:
                data_in = self.socket.recv(4096)
                if len(data_in) > 0:
                    if qortal_local_trace_enabled(self):
                        now = time.monotonic()
                        previous = self.qortal_trace_last_recv_at
                        gap_ms = (now - previous) * 1000.0 if previous else 0.0
                        self.qortal_trace_last_recv_at = now
                        if gap_ms >= QORTAL_RNS_LOCAL_TRACE_GAP_MS:
                            qortal_local_trace_log(
                                "local-socket-recv-gap",
                                f"role={qortal_local_trace_role(self)} interface={self} "
                                f"gap_ms={gap_ms:.3f} len={len(data_in)}"
                            )
                    self.handle_hdlc(data_in)
                else:
                    self.online = False
                    if self.is_connected_to_shared_instance and not self.detached:
                        RNS.log("Socket for "+str(self)+" was closed, attempting to reconnect...", RNS.LOG_WARNING)
                        RNS.Transport.shared_connection_disappeared()
                        # TODO: Potentially run this in a thread, but since if we get here,
                        # there's no other connectivity left to block anyway, it might be
                        # unnecessary.
                        self.reconnect()
                    else:
                        self.teardown(nowarning=True)

                    break

        except Exception as e:
            self.online = False
            RNS.log("An interface error occurred, the contained exception was: "+str(e), RNS.LOG_ERROR)
            RNS.log("Tearing down "+str(self), RNS.LOG_ERROR)
            self.teardown()

    def detach(self):
        if self.socket != None:
            if hasattr(self.socket, "close"):
                if callable(self.socket.close):
                    RNS.log("Detaching "+str(self), RNS.LOG_DEBUG)
                    self.detached = True

                    try:
                        if self.selector_backend and self.socket != None:
                            LocalSelectorManager._remove_client(self, self.socket)
                    except Exception as e:
                        RNS.log("Error while unregistering selector socket for "+str(self)+": "+str(e))

                    try:
                        if self.socket != None:
                            self.socket.shutdown(socket.SHUT_RDWR)
                    except Exception as e:
                        RNS.log("Error while shutting down socket for "+str(self)+": "+str(e))

                    try:
                        if self.socket != None:
                            self.socket.close()
                    except Exception as e:
                        RNS.log("Error while closing socket for "+str(self)+": "+str(e))

                    self.socket = None

    def teardown(self, nowarning=False):
        self.online = False
        self.OUT = False
        self.IN = False

        try:
            if self.selector_backend and self.socket != None:
                LocalSelectorManager._remove_client(self, self.socket)
        except Exception as e:
            RNS.log("Error while unregistering selector socket for "+str(self)+": "+str(e), RNS.LOG_DEBUG)

        RNS.Transport.remove_interface(self)

        if self in RNS.Transport.local_client_interfaces:
            RNS.Transport.local_client_interfaces.remove(self)
            if hasattr(self, "parent_interface") and self.parent_interface != None:
                self.parent_interface.clients -= 1
                if hasattr(RNS.Transport, "owner") and RNS.Transport.owner != None:
                    background = not self.detached
                    RNS.Transport.owner._should_persist_data(background=background)

        if nowarning == False:
            RNS.log("The interface "+str(self)+" experienced an unrecoverable error and is being torn down. Restart Reticulum to attempt to open this interface again.", RNS.LOG_ERROR)
            if RNS.Reticulum.panic_on_interface_error:
                RNS.panic()

        if self.is_connected_to_shared_instance:
            if nowarning == False:
                RNS.log("Permanently lost connection to local shared RNS instance. Exiting now.", RNS.LOG_CRITICAL)

            RNS.exit()


    def __str__(self):
        if self.socket_path: return "LocalInterface["+str(self.socket_path.replace("\0", ""))+"]"
        else: return "LocalInterface["+str(self.target_port)+"]"


class LocalServerInterface(Interface):
    AUTOCONFIGURE_MTU = True

    def __init__(self, owner, bindport=None, socket_path=None):
        super().__init__()
        self.epoll_backend = False
        self.selector_backend = False
        self.online = False
        self.clients = 0
        self.spawned_interfaces = []

        if socket_path != None and RNS.Reticulum.get_instance().use_af_unix: self.socket_path = f"\0rns/{socket_path}"
        else: self.socket_path = None

        self.IN  = True
        self.OUT = False
        self.name = "Reticulum"
        self.mode = RNS.Interfaces.Interface.Interface.MODE_FULL

        if RNS.vendor.platformutils.use_epoll():
            self.epoll_backend = True
        elif LocalSelectorManager.supported():
            self.selector_backend = True

        if socket_path != None and (self.epoll_backend or self.selector_backend):
            self.receives = True
            self.bind_ip = None
            self.bind_port = None

            self.owner = owner
            self.is_local_shared_instance = True
            if self.epoll_backend:
                BackboneInterface.add_listener(self, self.socket_path, socket_type=socket.AF_UNIX)
            else:
                LocalSelectorManager.add_listener(self, self.socket_path, socket_type=socket.AF_UNIX)

        elif bindport != None:
            self.receives = True
            self.bind_ip = "127.0.0.1"
            self.bind_port = bindport

            self.owner = owner
            self.is_local_shared_instance = True

            address = (self.bind_ip, self.bind_port)
            if self.epoll_backend: BackboneInterface.add_listener(self, address)
            elif self.selector_backend: LocalSelectorManager.add_listener(self, address)
            else:
                def handlerFactory(callback):
                    def createHandler(*args, **keys):
                        return LocalInterfaceHandler(callback, *args, **keys)
                    return createHandler

                self.server = ThreadingTCPServer(address, handlerFactory(self.incoming_connection))
                self.server.daemon_threads = True
                thread = threading.Thread(target=self.server.serve_forever)
                thread.daemon = True
                thread.start()

        self.announce_rate_target  = None
        self.announce_rate_grace   = None
        self.announce_rate_penalty = None

        self.bitrate = 1000*1000*1000
        self.online = True

    def incoming_connection(self, handler):
        if self.epoll_backend or self.selector_backend:
            client_socket = handler
            if client_socket.family == socket.AF_INET:
                interface_name = str(str(client_socket.getpeername()[1]))
            elif client_socket.family == socket.AF_UNIX:
                interface_name = f"{self.clients}@{self.socket_path}"

            spawned_interface = LocalClientInterface(self.owner, name=interface_name, connected_socket=client_socket)
            spawned_interface.qortal_trace_role = "daemon"
            spawned_interface.OUT = self.OUT
            spawned_interface.IN  = self.IN
            spawned_interface.socket = client_socket
            spawned_interface.parent_interface = self
            spawned_interface.bitrate = self.bitrate

            if client_socket.family == socket.AF_INET:
                spawned_interface.target_ip = client_socket.getpeername()[0]
                spawned_interface.target_port = str(client_socket.getpeername()[1])

            elif client_socket.family == socket.AF_UNIX:
                spawned_interface.target_ip = None
                spawned_interface.target_port = interface_name
                spawned_interface.socket_path = self.socket_path

            if hasattr(self, "_force_bitrate"): spawned_interface._force_bitrate = self._force_bitrate
            RNS.Transport.add_interface(spawned_interface)
            RNS.Transport.local_client_interfaces.append(spawned_interface)
            self.spawned_interfaces.append(spawned_interface)
            if QORTAL_RNS_LOCAL_TRACE_FRAMES:
                qortal_local_trace_log(
                    "local-daemon-client-attached",
                    f"role={qortal_local_trace_role(spawned_interface)} interface={spawned_interface} "
                    f"parent={self} clients={self.clients + 1}"
                )
            if self.epoll_backend:
                BackboneInterface.add_client_socket(client_socket, spawned_interface)
            else:
                LocalSelectorManager.add_client_socket(client_socket, spawned_interface)
            self.clients += 1
            return True

        else:
            interface_name = str(str(handler.client_address[1]))
            spawned_interface = LocalClientInterface(self.owner, name=interface_name, connected_socket=handler.request)
            spawned_interface.qortal_trace_role = "daemon"
            spawned_interface.OUT = self.OUT
            spawned_interface.IN  = self.IN
            spawned_interface.target_ip = handler.client_address[0]
            spawned_interface.target_port = str(handler.client_address[1])
            spawned_interface.parent_interface = self
            spawned_interface.bitrate = self.bitrate
            if hasattr(self, "_force_bitrate"): spawned_interface._force_bitrate = self._force_bitrate
            RNS.Transport.add_interface(spawned_interface)
            RNS.Transport.local_client_interfaces.append(spawned_interface)
            self.spawned_interfaces.append(spawned_interface)
            self.clients += 1
            if QORTAL_RNS_LOCAL_TRACE_FRAMES:
                qortal_local_trace_log(
                    "local-daemon-client-attached",
                    f"role={qortal_local_trace_role(spawned_interface)} interface={spawned_interface} "
                    f"parent={self} clients={self.clients}"
                )
            spawned_interface.read_loop()

    def process_outgoing(self, data):
        pass

    def received_announce(self, from_spawned=False):
        if from_spawned: self.ia_freq_deque.append(time.time())

    def sent_announce(self, from_spawned=False):
        if from_spawned: self.oa_freq_deque.append(time.time())

    def received_path_request(self, from_spawned=False):
        if from_spawned: self.ip_freq_deque.append(time.time())

    def sent_path_request(self, from_spawned=False):
        if from_spawned: self.op_freq_deque.append(time.time())

    def __str__(self):
        if self.socket_path: return "Shared Instance["+str(self.socket_path.replace("\0", ""))+"]"
        else: return "Shared Instance["+str(self.bind_port)+"]"

class LocalInterfaceHandler(socketserver.BaseRequestHandler):
    def __init__(self, callback, *args, **keys):
        self.callback = callback
        socketserver.BaseRequestHandler.__init__(self, *args, **keys)

    def handle(self):
        self.callback(handler=self)
