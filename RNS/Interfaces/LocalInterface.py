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
from RNS.Interfaces.EventedSocketIO import EventedSocketIO
from collections import deque
import asyncio
import socketserver
import threading
import socket
import select
import time
import sys
import os
import RNS
from threading import Lock, Condition

def env_int(name, default, minimum=None):
    try:
        value = int(os.environ.get(name, str(default)))
    except Exception:
        value = default

    if minimum != None and value < minimum:
        value = minimum

    return value

_qortal_dedicated_local_io_env = os.environ.get("QORTAL_RNS_DEDICATED_LOCAL_IO")
_qortal_local_io_backend_env = os.environ.get("QORTAL_RNS_LOCAL_IO_BACKEND", "auto").strip().lower()
if _qortal_local_io_backend_env not in ("auto", "dedicated", "selector", "epoll", "iocp"):
    _qortal_local_io_backend_env = "auto"

def _qortal_windows_iocp_available():
    return RNS.vendor.platformutils.is_windows() and (
        hasattr(asyncio, "ProactorEventLoop") or hasattr(asyncio, "WindowsProactorEventLoopPolicy")
    )

def _qortal_select_local_io_backend():
    if _qortal_dedicated_local_io_env != None and _qortal_dedicated_local_io_env != "0":
        return "dedicated"

    if _qortal_local_io_backend_env == "dedicated":
        return "dedicated"

    if _qortal_local_io_backend_env == "iocp":
        if _qortal_windows_iocp_available():
            return "iocp"
        return "dedicated"

    if _qortal_local_io_backend_env in ("selector", "epoll"):
        return "evented"

    if _qortal_dedicated_local_io_env == "0":
        return "evented"

    if RNS.vendor.platformutils.is_windows():
        if _qortal_windows_iocp_available():
            return "iocp"
        return "dedicated"

    return "evented"

QORTAL_RNS_LOCAL_IO_BACKEND = _qortal_select_local_io_backend()
if QORTAL_RNS_LOCAL_IO_BACKEND == "dedicated":
    QORTAL_RNS_DEDICATED_LOCAL_IO = True
else:
    QORTAL_RNS_DEDICATED_LOCAL_IO = False
QORTAL_RNS_IOCP_LOCAL_IO = QORTAL_RNS_LOCAL_IO_BACKEND == "iocp"
QORTAL_RNS_EVENTED_LOCAL_IO = QORTAL_RNS_LOCAL_IO_BACKEND == "evented"
QORTAL_RNS_RX_TRACE = os.environ.get("QORTAL_RNS_RX_TRACE", "0") == "1"
try:
    QORTAL_RNS_RX_TRACE_GAP_MS = int(os.environ.get("QORTAL_RNS_RX_TRACE_GAP_MS", "200"))
except Exception:
    QORTAL_RNS_RX_TRACE_GAP_MS = 200
try:
    QORTAL_RNS_DEDICATED_TX_QUEUE_MAX_BYTES = int(os.environ.get("QORTAL_RNS_DEDICATED_TX_QUEUE_MAX_BYTES", str(64*1024*1024)))
except Exception:
    QORTAL_RNS_DEDICATED_TX_QUEUE_MAX_BYTES = 64*1024*1024
QORTAL_RNS_IOCP_READ_BUDGET_BYTES = env_int("QORTAL_RNS_IOCP_READ_BUDGET_BYTES", 262144, 1024)
QORTAL_RNS_IOCP_WRITE_BUDGET_BYTES = env_int("QORTAL_RNS_IOCP_WRITE_BUDGET_BYTES", 262144, 1024)
QORTAL_RNS_IOCP_TX_QUEUE_MAX_BYTES = env_int("QORTAL_RNS_IOCP_TX_QUEUE_MAX_BYTES", 64*1024*1024, 1024)
QORTAL_RNS_LOCAL_IO_STATS = os.environ.get("QORTAL_RNS_LOCAL_IO_STATS", "0") == "1"
try:
    QORTAL_RNS_LOCAL_SEND_TIMEOUT_MS = int(os.environ.get("QORTAL_RNS_LOCAL_SEND_TIMEOUT_MS", "1500"))
except Exception:
    QORTAL_RNS_LOCAL_SEND_TIMEOUT_MS = 1500
if QORTAL_RNS_LOCAL_SEND_TIMEOUT_MS < 100:
    QORTAL_RNS_LOCAL_SEND_TIMEOUT_MS = 100
try:
    QORTAL_RNS_LOCAL_SEND_CHUNK_SIZE = int(os.environ.get("QORTAL_RNS_LOCAL_SEND_CHUNK_SIZE", str(16*1024)))
except Exception:
    QORTAL_RNS_LOCAL_SEND_CHUNK_SIZE = 16*1024
if QORTAL_RNS_LOCAL_SEND_CHUNK_SIZE < 1024:
    QORTAL_RNS_LOCAL_SEND_CHUNK_SIZE = 1024

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

class _IOCPClientState:
    def __init__(self, interface, client_socket):
        self.interface = interface
        self.socket = client_socket
        self.lock = Lock()
        self.queue = deque()
        self.queue_bytes = 0
        self.write_event = None
        self.closed = False
        self.read_task = None
        self.write_task = None

class WindowsIOCPSharedIO:
    loop = None
    thread = None
    started = threading.Event()
    start_lock = Lock()
    states_lock = Lock()
    listener_sockets = {}
    client_states_by_fileno = {}
    client_states_by_interface = {}
    stats_lock = Lock()
    last_stats_log = 0
    read_events = 0
    write_events = 0
    read_bytes = 0
    write_bytes = 0
    read_budget_hits = 0
    write_budget_hits = 0
    tx_queue_max = 0
    socket_close_count = 0
    socket_error_count = 0
    fallback_count = 0
    loop_slow_ticks = 0

    @staticmethod
    def available():
        return _qortal_windows_iocp_available()

    @staticmethod
    def start():
        if WindowsIOCPSharedIO.loop != None:
            return True

        with WindowsIOCPSharedIO.start_lock:
            if WindowsIOCPSharedIO.loop != None:
                return True

            WindowsIOCPSharedIO.started.clear()

            def run_loop():
                try:
                    if hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
                        loop = asyncio.WindowsProactorEventLoopPolicy().new_event_loop()
                    elif hasattr(asyncio, "ProactorEventLoop"):
                        loop = asyncio.ProactorEventLoop()
                    else:
                        loop = asyncio.new_event_loop()

                    WindowsIOCPSharedIO.loop = loop
                    asyncio.set_event_loop(loop)
                    loop.create_task(WindowsIOCPSharedIO._stats_loop())
                    WindowsIOCPSharedIO.started.set()
                    loop.run_forever()

                except Exception as e:
                    WindowsIOCPSharedIO._note_stat("fallback_count")
                    RNS.log(f"Windows IOCP local shared I/O loop failed to start: {e}", RNS.LOG_ERROR)
                    RNS.trace_exception(e)
                    WindowsIOCPSharedIO.started.set()

            WindowsIOCPSharedIO.thread = threading.Thread(target=run_loop, daemon=True)
            WindowsIOCPSharedIO.thread.start()
            WindowsIOCPSharedIO.started.wait(3.0)

            if WindowsIOCPSharedIO.loop == None:
                WindowsIOCPSharedIO._note_stat("fallback_count")
                return False

            RNS.log("Using Windows IOCP local shared I/O backend", RNS.LOG_INFO)
            return True

    @staticmethod
    def _note_stat(name, value=1):
        if not QORTAL_RNS_LOCAL_IO_STATS and name != "fallback_count":
            return

        with WindowsIOCPSharedIO.stats_lock:
            setattr(WindowsIOCPSharedIO, name, getattr(WindowsIOCPSharedIO, name) + value)

    @staticmethod
    def _note_tx_queue(length):
        if not QORTAL_RNS_LOCAL_IO_STATS:
            return

        with WindowsIOCPSharedIO.stats_lock:
            if length > WindowsIOCPSharedIO.tx_queue_max:
                WindowsIOCPSharedIO.tx_queue_max = length

    @staticmethod
    async def _stats_loop():
        while True:
            started = time.time()
            await asyncio.sleep(10)
            if time.time() - started > 11:
                WindowsIOCPSharedIO._note_stat("loop_slow_ticks")
            WindowsIOCPSharedIO._maybe_log_stats()

    @staticmethod
    def _maybe_log_stats():
        if not QORTAL_RNS_LOCAL_IO_STATS:
            return

        now = time.time()
        with WindowsIOCPSharedIO.stats_lock:
            if now < WindowsIOCPSharedIO.last_stats_log + 10:
                return

            WindowsIOCPSharedIO.last_stats_log = now
            stats = (
                WindowsIOCPSharedIO.read_events,
                WindowsIOCPSharedIO.write_events,
                WindowsIOCPSharedIO.read_bytes,
                WindowsIOCPSharedIO.write_bytes,
                WindowsIOCPSharedIO.read_budget_hits,
                WindowsIOCPSharedIO.write_budget_hits,
                WindowsIOCPSharedIO.tx_queue_max,
                WindowsIOCPSharedIO.socket_close_count,
                WindowsIOCPSharedIO.socket_error_count,
                WindowsIOCPSharedIO.fallback_count,
                WindowsIOCPSharedIO.loop_slow_ticks,
            )

        RNS.log(
            f"local event I/O stats backend=iocp "
            f"read_events={stats[0]} write_events={stats[1]} "
            f"read_bytes={stats[2]} write_bytes={stats[3]} "
            f"read_budget_hits={stats[4]} write_budget_hits={stats[5]} "
            f"tx_queue_max={stats[6]} socket_closes={stats[7]} "
            f"socket_errors={stats[8]} fallback_count={stats[9]} "
            f"loop_slow_ticks={stats[10]}",
            RNS.LOG_NOTICE,
        )

    @staticmethod
    def add_listener(interface, bind_address, socket_type=socket.AF_INET):
        if not WindowsIOCPSharedIO.start():
            raise OSError("Windows IOCP local shared I/O backend is unavailable")

        if socket_type == socket.AF_INET:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        elif socket_type == socket.AF_INET6:
            server_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:
            raise TypeError(f"Invalid socket type {socket_type} for Windows IOCP local shared I/O")

        server_socket.bind(bind_address)
        server_socket.listen(64)
        server_socket.setblocking(False)
        WindowsIOCPSharedIO.listener_sockets[server_socket.fileno()] = (interface, server_socket)
        WindowsIOCPSharedIO.loop.call_soon_threadsafe(
            lambda: WindowsIOCPSharedIO.loop.create_task(WindowsIOCPSharedIO._accept_loop(interface, server_socket))
        )
        return server_socket

    @staticmethod
    async def _accept_loop(interface, server_socket):
        while True:
            try:
                client_socket, address = await WindowsIOCPSharedIO.loop.sock_accept(server_socket)
                client_socket.setblocking(False)
                if client_socket.family == socket.AF_INET:
                    client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                if not interface.incoming_connection(client_socket):
                    try: client_socket.close()
                    except Exception as e: RNS.log(f"Error while closing failed IOCP incoming socket: {e}", RNS.LOG_WARNING)

            except asyncio.CancelledError:
                break
            except Exception as e:
                WindowsIOCPSharedIO._note_stat("socket_error_count")
                RNS.log(f"Accepting Windows IOCP local socket failed for {interface}: {e}", RNS.LOG_WARNING)
                await asyncio.sleep(0.25)

    @staticmethod
    def add_client_socket(client_socket, interface):
        if not WindowsIOCPSharedIO.start():
            return False

        try:
            client_socket.setblocking(False)
        except Exception as e:
            RNS.log(f"Could not set IOCP local shared socket to nonblocking mode: {e}", RNS.LOG_WARNING)
            return False

        state = _IOCPClientState(interface, client_socket)
        fileno = client_socket.fileno()
        with WindowsIOCPSharedIO.states_lock:
            WindowsIOCPSharedIO.client_states_by_fileno[fileno] = state
            WindowsIOCPSharedIO.client_states_by_interface[interface] = state
        WindowsIOCPSharedIO.loop.call_soon_threadsafe(WindowsIOCPSharedIO._start_client_state, state)
        return True

    @staticmethod
    def _start_client_state(state):
        if state.closed:
            return

        state.write_event = asyncio.Event()
        state.read_task = WindowsIOCPSharedIO.loop.create_task(WindowsIOCPSharedIO._read_loop(state))
        state.write_task = WindowsIOCPSharedIO.loop.create_task(WindowsIOCPSharedIO._write_loop(state))
        with state.lock:
            if len(state.queue) > 0:
                state.write_event.set()

    @staticmethod
    def queue_outgoing(interface, data):
        with WindowsIOCPSharedIO.states_lock:
            state = WindowsIOCPSharedIO.client_states_by_interface.get(interface)
        if state == None:
            return False

        with state.lock:
            if state.closed or not interface.online or interface.detached:
                return False

            queued_bytes = state.queue_bytes + len(data)
            if queued_bytes > QORTAL_RNS_IOCP_TX_QUEUE_MAX_BYTES:
                WindowsIOCPSharedIO._note_stat("write_budget_hits")
                RNS.log(
                    f"IOCP TX queue for {interface} is full, dropping outbound packet "
                    f"queued={state.queue_bytes} packet={len(data)} "
                    f"limit={QORTAL_RNS_IOCP_TX_QUEUE_MAX_BYTES}",
                    RNS.LOG_WARNING,
                )
                return False

            state.queue.append(data)
            state.queue_bytes = queued_bytes
            WindowsIOCPSharedIO._note_tx_queue(state.queue_bytes)
            write_event = state.write_event

        if write_event != None and WindowsIOCPSharedIO.loop != None:
            WindowsIOCPSharedIO.loop.call_soon_threadsafe(write_event.set)

        return True

    @staticmethod
    async def _read_loop(state):
        while not state.closed:
            read_total = 0
            try:
                while not state.closed and read_total < QORTAL_RNS_IOCP_READ_BUDGET_BYTES:
                    read_size = min(state.interface.HW_MTU, QORTAL_RNS_IOCP_READ_BUDGET_BYTES - read_total)
                    received = await WindowsIOCPSharedIO.loop.sock_recv(state.socket, read_size)
                    WindowsIOCPSharedIO._note_stat("read_events")
                    if len(received) == 0:
                        WindowsIOCPSharedIO.close_state(state, error=False)
                        return

                    read_total += len(received)
                    WindowsIOCPSharedIO._note_stat("read_bytes", len(received))
                    state.interface.receive(received)

                    if len(received) < read_size:
                        break

                if read_total >= QORTAL_RNS_IOCP_READ_BUDGET_BYTES:
                    WindowsIOCPSharedIO._note_stat("read_budget_hits")
                    await asyncio.sleep(0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                if not state.interface.detached:
                    RNS.log(f"Error while reading Windows IOCP local socket for {state.interface}: {e}", RNS.LOG_DEBUG)
                WindowsIOCPSharedIO.close_state(state, error=True)
                return

    @staticmethod
    async def _write_loop(state):
        while not state.closed:
            try:
                if state.write_event == None:
                    await asyncio.sleep(0)
                    continue

                await state.write_event.wait()
                state.write_event.clear()

                while not state.closed:
                    with state.lock:
                        if len(state.queue) == 0:
                            break

                        data = state.queue.popleft()
                        state.queue_bytes -= len(data)

                    offset = 0
                    while offset < len(data) and not state.closed:
                        chunk_end = min(offset + QORTAL_RNS_IOCP_WRITE_BUDGET_BYTES, len(data))
                        chunk = data[offset:chunk_end]
                        await WindowsIOCPSharedIO.loop.sock_sendall(state.socket, chunk)
                        WindowsIOCPSharedIO._note_stat("write_events")
                        WindowsIOCPSharedIO._note_stat("write_bytes", len(chunk))
                        state.interface.txb += len(chunk)
                        if state.interface.parent_interface != None:
                            state.interface.parent_interface.txb += len(chunk)
                        offset = chunk_end
                        if offset < len(data):
                            WindowsIOCPSharedIO._note_stat("write_budget_hits")
                            await asyncio.sleep(0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                if not state.interface.detached:
                    RNS.log(f"Error while writing Windows IOCP local socket for {state.interface}: {e}", RNS.LOG_DEBUG)
                WindowsIOCPSharedIO.close_state(state, error=True)
                return

    @staticmethod
    def close_state(state, error=False):
        with state.lock:
            if state.closed:
                return

            state.closed = True
            state.queue.clear()
            state.queue_bytes = 0
            write_event = state.write_event

        if write_event != None and WindowsIOCPSharedIO.loop != None:
            try:
                WindowsIOCPSharedIO.loop.call_soon_threadsafe(write_event.set)
            except Exception:
                pass

        if error:
            WindowsIOCPSharedIO._note_stat("socket_error_count")
        else:
            WindowsIOCPSharedIO._note_stat("socket_close_count")

        with WindowsIOCPSharedIO.states_lock:
            try:
                fileno = state.socket.fileno()
                WindowsIOCPSharedIO.client_states_by_fileno.pop(fileno, None)
            except Exception:
                pass

            try:
                WindowsIOCPSharedIO.client_states_by_interface.pop(state.interface, None)
            except Exception:
                pass

        pif = None
        try:
            if state.interface.parent_interface:
                pif = state.interface.parent_interface
                if pif.spawned_interfaces != None:
                    while state.interface in pif.spawned_interfaces:
                        pif.spawned_interfaces.remove(state.interface)
        except Exception as e:
            RNS.log(f"Error while removing spawned interface from {pif}: {e}", RNS.LOG_ERROR)

        try:
            state.socket.close()
        except Exception as e:
            RNS.log(f"Error while closing Windows IOCP socket for {state.interface}: {e}", RNS.LOG_WARNING)

        def notify_close():
            try:
                state.interface.receive(b"")
            except Exception as e:
                RNS.log(f"Error while notifying {state.interface} of Windows IOCP socket close: {e}", RNS.LOG_DEBUG)

        threading.Thread(target=notify_close, daemon=True).start()

class LocalClientInterface(Interface):
    RECONNECT_WAIT = 8
    AUTOCONFIGURE_MTU = True
    CLIENT_SLEEP_PAUSE_TIMEOUT = 12

    def __init__(self, owner, name, target_port = None, connected_socket=None, socket_path=None):
        super().__init__()

        self.epoll_backend    = False
        self.iocp_backend     = False
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
        self.frame_buffer     = b""
        self.transmit_buffer  = b""
        self.writing          = False
        self._force_bitrate   = False
        self.send_lock        = Lock()
        self.tx_queue         = deque()
        self.tx_queue_bytes   = 0
        self.tx_queue_cv      = Condition()
        self.tx_worker_started = False
        self.tx_shutdown      = False

        if QORTAL_RNS_IOCP_LOCAL_IO: self.iocp_backend = True
        elif QORTAL_RNS_EVENTED_LOCAL_IO: self.epoll_backend = True

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

        self.announce_rate_target  = None
        self.announce_rate_grace   = None
        self.announce_rate_penalty = None

        if not self.epoll_backend and not self.iocp_backend and QORTAL_RNS_DEDICATED_LOCAL_IO:
            self.start_dedicated_write_loop()

        if connected_socket == None:
            if not self.epoll_backend and not self.iocp_backend:
                if QORTAL_RNS_DEDICATED_LOCAL_IO:
                    RNS.log(f"Using dedicated local shared I/O thread for {self}", RNS.LOG_INFO)
                thread = threading.Thread(target=self.read_loop)
                thread.daemon = True
                thread.start()

    def should_ingress_limit(self):
        return False

    def start_dedicated_write_loop(self):
        with self.tx_queue_cv:
            if self.tx_worker_started:
                return
            self.tx_worker_started = True
        thread = threading.Thread(target=self.write_loop)
        thread.daemon = True
        thread.start()

    def enqueue_outgoing(self, data):
        with self.tx_queue_cv:
            if self.tx_shutdown or not self.online or self.detached:
                return False

            queued_bytes = self.tx_queue_bytes + len(data)
            if queued_bytes > QORTAL_RNS_DEDICATED_TX_QUEUE_MAX_BYTES:
                RNS.log(
                    f"TX queue for {self} is full, dropping outbound packet "
                    f"queued={self.tx_queue_bytes} packet={len(data)} "
                    f"limit={QORTAL_RNS_DEDICATED_TX_QUEUE_MAX_BYTES}",
                    RNS.LOG_WARNING,
                )
                return False

            self.tx_queue.append(data)
            self.tx_queue_bytes = queued_bytes
            self.tx_queue_cv.notify()
            return True

    def send_bounded(self, data):
        if self.socket == None:
            raise IOError("Cannot transmit on closed local interface socket")

        if not hasattr(socket, "MSG_DONTWAIT"):
            timeout_s = QORTAL_RNS_LOCAL_SEND_TIMEOUT_MS / 1000.0
            previous_timeout = self.socket.gettimeout()
            try:
                self.socket.settimeout(timeout_s)
                self.socket.sendall(data)
            finally:
                self.socket.settimeout(previous_timeout)
            return

        total_sent = 0
        deadline = time.time() + (QORTAL_RNS_LOCAL_SEND_TIMEOUT_MS / 1000.0)
        while total_sent < len(data):
            remaining = deadline - time.time()
            if remaining <= 0:
                raise TimeoutError(f"Timed out sending {len(data)} bytes on {self}")

            try:
                _, writable, _ = select.select([], [self.socket], [], min(remaining, 0.05))
            except (OSError, ValueError):
                raise

            if not writable:
                continue

            chunk_end = min(total_sent + QORTAL_RNS_LOCAL_SEND_CHUNK_SIZE, len(data))
            try:
                sent = self.socket.send(data[total_sent:chunk_end], socket.MSG_DONTWAIT)
            except (BlockingIOError, InterruptedError):
                continue

            if sent == 0:
                raise IOError(f"Socket closed while sending on {self}")

            total_sent += sent

    def write_loop(self):
        while True:
            with self.tx_queue_cv:
                while not self.tx_shutdown and not self.detached and (not self.online or len(self.tx_queue) == 0):
                    self.tx_queue_cv.wait(1.0)

                if self.tx_shutdown or self.detached:
                    self.tx_queue.clear()
                    self.tx_queue_bytes = 0
                    self.tx_worker_started = False
                    return

                data = self.tx_queue.popleft()
                self.tx_queue_bytes -= len(data)

            try:
                if self._force_bitrate:
                    s = len(data) / self.bitrate * 8
                    time.sleep(s)

                self.writing = True
                send_started = time.time()
                with self.send_lock:
                    self.send_bounded(data)
                self.writing = False
                self.txb += len(data)
                if hasattr(self, "parent_interface") and self.parent_interface != None:
                    self.parent_interface.txb += len(data)
                send_ms = int((time.time() - send_started) * 1000)
                if send_ms >= QORTAL_RNS_LOCAL_SEND_TIMEOUT_MS // 2:
                    RNS.log(f"LocalInterface send was slow on {self}: elapsed_ms={send_ms} bytes={len(data)}", RNS.LOG_WARNING)

            except Exception as e:
                self.writing = False
                RNS.log("Exception occurred while transmitting via "+str(self)+", tearing down interface", RNS.LOG_ERROR)
                RNS.log("The contained exception was: "+str(e), RNS.LOG_ERROR)
                RNS.trace_exception(e)
                self.teardown()
                self.tx_worker_started = False
                return

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
        self.never_connected = False
        with self.tx_queue_cv:
            self.tx_shutdown = False
            self.tx_queue_cv.notify_all()

        if RNS.vendor.platformutils.is_android(): self.phy_keepalive = True
        if self.iocp_backend:
            if not WindowsIOCPSharedIO.add_client_socket(self.socket, self):
                RNS.log(f"Windows IOCP local shared I/O unavailable for {self}, falling back to dedicated local I/O", RNS.LOG_WARNING)
                self.iocp_backend = False
                self.epoll_backend = False
                self.start_dedicated_write_loop()
                thread = threading.Thread(target=self.read_loop)
                thread.daemon = True
                thread.start()
        elif self.epoll_backend: EventedSocketIO.add_client_socket(self.socket, self)
        elif QORTAL_RNS_DEDICATED_LOCAL_IO: self.start_dedicated_write_loop()

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
                if not self.epoll_backend and not self.iocp_backend:
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
                if self.iocp_backend:
                    WindowsIOCPSharedIO.queue_outgoing(self, bytes([HDLC.FLAG])+bytes([HDLC.FLAG]))

                elif self.epoll_backend:
                    self.transmit_buffer += bytes([HDLC.FLAG])+bytes([HDLC.FLAG])
                    EventedSocketIO.tx_ready(self)

                elif QORTAL_RNS_DEDICATED_LOCAL_IO:
                    self.enqueue_outgoing(bytes([HDLC.FLAG])+bytes([HDLC.FLAG]))

                else:
                    self.writing = True
                    data = bytes([HDLC.FLAG])+bytes([HDLC.FLAG])
                    with self.send_lock:
                        self.send_bounded(data)
                    self.writing = False

            except Exception as e: RNS.log(f"Exception occurred while sending keepalive on {self}: {e}", RNS.LOG_ERROR)

    def process_incoming(self, data):
        if QORTAL_RNS_RX_TRACE:
            now = time.time()
            last_incoming = getattr(self, "_qortal_last_incoming_frame_ts", None)
            self._qortal_last_incoming_frame_ts = now
            if last_incoming != None:
                gap_ms = int((now - last_incoming) * 1000)
                if gap_ms >= QORTAL_RNS_RX_TRACE_GAP_MS:
                    RNS.log(f"[qortal_rx_trace] local_frame_gap gap_ms={gap_ms} bytes={len(data)} interface={self}", RNS.LOG_NOTICE)

            process_start = now

        self.rxb += len(data)
        if self.parent_interface != None: self.parent_interface.rxb += len(data)
        
        try: self.owner.inbound(data, self)
        except Exception as e:
            RNS.log(f"An error occurred in the processing of an incoming frame for {self}: {e}", RNS.LOG_ERROR)
            RNS.trace_exception(e)
        finally:
            if QORTAL_RNS_RX_TRACE:
                process_ms = int((time.time() - process_start) * 1000)
                if process_ms >= QORTAL_RNS_RX_TRACE_GAP_MS:
                    RNS.log(f"[qortal_rx_trace] local_frame_process_slow elapsed_ms={process_ms} bytes={len(data)} interface={self}", RNS.LOG_NOTICE)

    def process_outgoing(self, data):
        if self.pause_on_client_sleep and time.time() > self.pause_timeout:
            RNS.log(f"TX paused for LocalInterface client, dropping outbound packet", RNS.LOG_DEBUG) # TODO: Remove
            return

        if self.online:
            try:
                if self.iocp_backend:
                    framed_data = bytes([HDLC.FLAG])+HDLC.escape(data)+bytes([HDLC.FLAG])
                    WindowsIOCPSharedIO.queue_outgoing(self, framed_data)

                elif self.epoll_backend:
                    self.transmit_buffer += bytes([HDLC.FLAG])+HDLC.escape(data)+bytes([HDLC.FLAG])
                    EventedSocketIO.tx_ready(self)

                elif QORTAL_RNS_DEDICATED_LOCAL_IO:
                    framed_data = bytes([HDLC.FLAG])+HDLC.escape(data)+bytes([HDLC.FLAG])
                    self.enqueue_outgoing(framed_data)

                else:
                    self.writing = True

                    if self._force_bitrate:
                        if not hasattr(self, "send_lock"):
                            self.send_lock = Lock()

                        with self.send_lock:
                            # RNS.log(f"Simulating latency of {RNS.prettytime(s)} for {len(data)} bytes", RNS.LOG_EXTREME)
                            s = len(data) / self.bitrate * 8
                            time.sleep(s)

                    data = bytes([HDLC.FLAG])+HDLC.escape(data)+bytes([HDLC.FLAG])
                    with self.send_lock:
                        self.send_bounded(data)
                    self.writing = False
                    self.txb += len(data)
                    if hasattr(self, "parent_interface") and self.parent_interface != None:
                        self.parent_interface.txb += len(data)

            except Exception as e:
                self.writing = False
                RNS.log("Exception occurred while transmitting via "+str(self)+", tearing down interface", RNS.LOG_ERROR)
                RNS.log("The contained exception was: "+str(e), RNS.LOG_ERROR)
                RNS.trace_exception(e)
                self.teardown()

    def handle_hdlc(self, data_in):
        self.frame_buffer += data_in
        flags_remaining = True
        while flags_remaining:
            frame_start = self.frame_buffer.find(HDLC.FLAG)
            if frame_start != -1:
                frame_end = self.frame_buffer.find(HDLC.FLAG, frame_start+1)
                if frame_end != -1:
                    frame = self.frame_buffer[frame_start+1:frame_end]
                    frame = frame.replace(bytes([HDLC.ESC, HDLC.FLAG ^ HDLC.ESC_MASK]), bytes([HDLC.FLAG]))
                    frame = frame.replace(bytes([HDLC.ESC, HDLC.ESC  ^ HDLC.ESC_MASK]), bytes([HDLC.ESC]))
                    if len(frame) > RNS.Reticulum.HEADER_MINSIZE: self.process_incoming(frame)
                    self.frame_buffer = self.frame_buffer[frame_end:]
                
                else: flags_remaining = False
            
            else: flags_remaining = False

    def receive(self, data_in):
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

    def read_loop(self):
        try:
            self.frame_buffer = b""
            data_in = b""
            while True:
                data_in = self.socket.recv(4096)
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
                    with self.tx_queue_cv:
                        self.tx_shutdown = True
                        self.tx_queue_cv.notify_all()
                    
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
        with self.tx_queue_cv:
            self.tx_shutdown = True
            self.tx_queue_cv.notify_all()

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
        self.iocp_backend = False
        self.online = False
        self.clients = 0
        
        if socket_path != None and RNS.Reticulum.get_instance().use_af_unix: self.socket_path = f"\0rns/{socket_path}"
        else: self.socket_path = None
        
        self.IN  = True
        self.OUT = False
        self.name = "Reticulum"
        self.mode = RNS.Interfaces.Interface.Interface.MODE_FULL

        if QORTAL_RNS_IOCP_LOCAL_IO:
            self.iocp_backend = True
        elif QORTAL_RNS_EVENTED_LOCAL_IO:
            self.epoll_backend = True

        if socket_path != None and self.epoll_backend:
            self.receives = True
            self.bind_ip = None
            self.bind_port = None

            self.owner = owner
            self.is_local_shared_instance = True
            EventedSocketIO.add_listener(self, self.socket_path, socket_type=socket.AF_UNIX)

        elif bindport != None:
            self.receives = True
            self.bind_ip = "127.0.0.1"
            self.bind_port = bindport

            self.owner = owner
            self.is_local_shared_instance = True

            address = (self.bind_ip, self.bind_port)
            if self.iocp_backend:
                try:
                    self.server_socket = WindowsIOCPSharedIO.add_listener(self, address)
                except Exception as e:
                    if WindowsIOCPSharedIO.loop != None:
                        raise

                    WindowsIOCPSharedIO._note_stat("fallback_count")
                    RNS.log(f"Windows IOCP local shared listener unavailable, falling back to dedicated local I/O: {e}", RNS.LOG_WARNING)
                    self.iocp_backend = False
                    self.epoll_backend = False
                    self.server_socket = None
                    def handlerFactory(callback):
                        def createHandler(*args, **keys):
                            return LocalInterfaceHandler(callback, *args, **keys)
                        return createHandler

                    self.server = ThreadingTCPServer(address, handlerFactory(self.incoming_connection))
                    self.server.daemon_threads = True
                    thread = threading.Thread(target=self.server.serve_forever)
                    thread.daemon = True
                    thread.start()
            elif self.epoll_backend: EventedSocketIO.add_listener(self, address)
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
        if self.epoll_backend or self.iocp_backend:
            client_socket = handler
            if client_socket.family == socket.AF_INET:
                interface_name = str(str(client_socket.getpeername()[1]))
            elif client_socket.family == socket.AF_UNIX:
                interface_name = f"{self.clients}@{self.socket_path}"

            spawned_interface = LocalClientInterface(self.owner, name=interface_name, connected_socket=client_socket)
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
            if spawned_interface.iocp_backend:
                if not WindowsIOCPSharedIO.add_client_socket(client_socket, spawned_interface):
                    try: client_socket.setblocking(True)
                    except Exception as e: RNS.log(f"Could not set local shared client socket to blocking mode: {e}", RNS.LOG_WARNING)
                    spawned_interface.iocp_backend = False
                    spawned_interface.epoll_backend = False
                    spawned_interface.start_dedicated_write_loop()
                    thread = threading.Thread(target=spawned_interface.read_loop)
                    thread.daemon = True
                    thread.start()
            elif spawned_interface.epoll_backend:
                EventedSocketIO.add_client_socket(client_socket, spawned_interface)
            else:
                try: client_socket.setblocking(True)
                except Exception as e: RNS.log(f"Could not set local shared client socket to blocking mode: {e}", RNS.LOG_WARNING)
                if QORTAL_RNS_DEDICATED_LOCAL_IO:
                    RNS.log(f"Using dedicated local shared I/O thread for {spawned_interface}", RNS.LOG_INFO)
                thread = threading.Thread(target=spawned_interface.read_loop)
                thread.daemon = True
                thread.start()
            self.clients += 1
            return True

        else:
            interface_name = str(str(handler.client_address[1]))
            spawned_interface = LocalClientInterface(self.owner, name=interface_name, connected_socket=handler.request)
            spawned_interface.OUT = self.OUT
            spawned_interface.IN  = self.IN
            spawned_interface.target_ip = handler.client_address[0]
            spawned_interface.target_port = str(handler.client_address[1])
            spawned_interface.parent_interface = self
            spawned_interface.bitrate = self.bitrate
            if hasattr(self, "_force_bitrate"): spawned_interface._force_bitrate = self._force_bitrate
            spawned_interface.iocp_backend = False
            spawned_interface.epoll_backend = False
            spawned_interface.start_dedicated_write_loop()
            RNS.Transport.add_interface(spawned_interface)
            RNS.Transport.local_client_interfaces.append(spawned_interface)
            self.clients += 1
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
