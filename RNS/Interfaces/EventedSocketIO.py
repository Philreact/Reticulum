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

import errno
import os
import select
import selectors
import socket
import threading
import time
import RNS


def env_int(name, default, minimum=None):
    try:
        value = int(os.environ.get(name, str(default)))
    except Exception:
        value = default

    if minimum != None and value < minimum:
        value = minimum

    return value


QORTAL_RNS_EVENTED_IO_READ_BUDGET_BYTES = env_int("QORTAL_RNS_EPOLL_READ_BUDGET_BYTES", 262144, 1024)
QORTAL_RNS_EVENTED_IO_WRITE_BUDGET_BYTES = env_int("QORTAL_RNS_EPOLL_WRITE_BUDGET_BYTES", 262144, 1024)
QORTAL_RNS_LOCAL_IO_STATS = os.environ.get("QORTAL_RNS_LOCAL_IO_STATS", "0") == "1"
QORTAL_RNS_LOCAL_IO_BACKEND = os.environ.get("QORTAL_RNS_LOCAL_IO_BACKEND", "auto").strip().lower()


class EventedSocketIO:
    epoll = None
    selector = None
    event_backend = None
    listener_filenos = {}
    spawned_interface_filenos = {}
    _job_active = False
    _job_lock = threading.Lock()
    _stats_lock = threading.Lock()
    _last_stats_log = 0
    read_events = 0
    write_events = 0
    read_bytes = 0
    write_bytes = 0
    write_budget_hits = 0
    read_budget_hits = 0
    tx_buffer_max = 0
    socket_close_count = 0
    socket_error_count = 0

    @staticmethod
    def _hup_mask():
        if EventedSocketIO.event_backend != "epoll":
            return 0

        mask = select.EPOLLHUP | select.EPOLLERR
        if hasattr(select, "EPOLLRDHUP"):
            mask |= select.EPOLLRDHUP

        return mask

    @staticmethod
    def _read_mask():
        if EventedSocketIO.event_backend == "epoll":
            return select.EPOLLIN

        return selectors.EVENT_READ

    @staticmethod
    def _write_mask():
        if EventedSocketIO.event_backend == "epoll":
            return select.EPOLLOUT

        return selectors.EVENT_WRITE

    @staticmethod
    def _read_write_mask():
        return EventedSocketIO._read_mask() | EventedSocketIO._write_mask()

    @staticmethod
    def _is_read_event(event):
        return bool(event & EventedSocketIO._read_mask())

    @staticmethod
    def _is_write_event(event):
        return bool(event & EventedSocketIO._write_mask())

    @staticmethod
    def _note_stat(name, value=1):
        if not QORTAL_RNS_LOCAL_IO_STATS:
            return

        with EventedSocketIO._stats_lock:
            setattr(EventedSocketIO, name, getattr(EventedSocketIO, name) + value)

    @staticmethod
    def _note_tx_buffer(length):
        if not QORTAL_RNS_LOCAL_IO_STATS:
            return

        with EventedSocketIO._stats_lock:
            if length > EventedSocketIO.tx_buffer_max:
                EventedSocketIO.tx_buffer_max = length

    @staticmethod
    def _maybe_log_stats():
        if not QORTAL_RNS_LOCAL_IO_STATS:
            return

        now = time.time()
        with EventedSocketIO._stats_lock:
            if now < EventedSocketIO._last_stats_log + 10:
                return

            EventedSocketIO._last_stats_log = now
            stats = (
                EventedSocketIO.read_events,
                EventedSocketIO.write_events,
                EventedSocketIO.read_bytes,
                EventedSocketIO.write_bytes,
                EventedSocketIO.read_budget_hits,
                EventedSocketIO.write_budget_hits,
                EventedSocketIO.tx_buffer_max,
                EventedSocketIO.socket_close_count,
                EventedSocketIO.socket_error_count,
            )

        RNS.log(
            f"local event I/O stats backend={EventedSocketIO.event_backend} "
            f"read_events={stats[0]} write_events={stats[1]} "
            f"read_bytes={stats[2]} write_bytes={stats[3]} "
            f"read_budget_hits={stats[4]} write_budget_hits={stats[5]} "
            f"tx_buffer_max={stats[6]} socket_closes={stats[7]} socket_errors={stats[8]}",
            RNS.LOG_NOTICE,
        )

    @staticmethod
    def start():
        with EventedSocketIO._job_lock:
            if EventedSocketIO._job_active:
                return

            EventedSocketIO._job_active = True
            threading.Thread(target=EventedSocketIO.__job, daemon=True).start()

    @staticmethod
    def ensure_backend():
        if EventedSocketIO.event_backend:
            return

        if QORTAL_RNS_LOCAL_IO_BACKEND == "selector":
            EventedSocketIO.selector = selectors.DefaultSelector()
            EventedSocketIO.event_backend = EventedSocketIO.selector.__class__.__name__
        elif RNS.vendor.platformutils.use_epoll():
            EventedSocketIO.epoll = select.epoll()
            EventedSocketIO.event_backend = "epoll"
        else:
            EventedSocketIO.selector = selectors.DefaultSelector()
            EventedSocketIO.event_backend = EventedSocketIO.selector.__class__.__name__

    @staticmethod
    def _register_fileno(fileno, mask):
        if EventedSocketIO.event_backend == "epoll":
            EventedSocketIO.epoll.register(fileno, mask)
        else:
            EventedSocketIO.selector.register(fileno, mask)

    @staticmethod
    def _modify_fileno(fileno, mask):
        if EventedSocketIO.event_backend == "epoll":
            EventedSocketIO.epoll.modify(fileno, mask)
        else:
            EventedSocketIO.selector.modify(fileno, mask)

    @staticmethod
    def _fileno_not_registered_error(error):
        if isinstance(error, KeyError):
            return True

        if isinstance(error, OSError):
            return getattr(error, "errno", None) == errno.ENOENT

        return False

    @staticmethod
    def _fileno_already_registered_error(error):
        if isinstance(error, KeyError):
            return "already registered" in str(error)

        if isinstance(error, OSError):
            return getattr(error, "errno", None) == errno.EEXIST

        return False

    @staticmethod
    def _register_or_recover_fileno(fileno, mask, owner=None):
        try:
            EventedSocketIO._register_fileno(fileno, mask)
            return True
        except Exception as e:
            if not EventedSocketIO._fileno_already_registered_error(e):
                raise e

            try:
                EventedSocketIO._modify_fileno(fileno, mask)
                RNS.log(f"Recovered already registered local I/O file descriptor {fileno} for {owner}", RNS.LOG_DEBUG)
                return True
            except Exception as modify_error:
                RNS.log(f"Unable to recover already registered local I/O file descriptor {fileno} for {owner}: {modify_error}", RNS.LOG_DEBUG)
                return False

    @staticmethod
    def _modify_or_recover_fileno(fileno, mask, owner=None):
        try:
            EventedSocketIO._modify_fileno(fileno, mask)
            return True
        except Exception as e:
            if not EventedSocketIO._fileno_not_registered_error(e):
                raise e

            try:
                if not EventedSocketIO._register_or_recover_fileno(fileno, mask, owner):
                    return False
                RNS.log(f"Recovered unregistered local I/O file descriptor {fileno} for {owner}", RNS.LOG_DEBUG)
                return True
            except Exception as register_error:
                RNS.log(f"Unable to recover unregistered local I/O file descriptor {fileno} for {owner}: {register_error}", RNS.LOG_DEBUG)
                return False

    @staticmethod
    def _unregister_fileno(fileno):
        if EventedSocketIO.event_backend == "epoll":
            EventedSocketIO.epoll.unregister(fileno)
        else:
            EventedSocketIO.selector.unregister(fileno)

    @staticmethod
    def _poll(timeout):
        if EventedSocketIO.event_backend == "epoll":
            return EventedSocketIO.epoll.poll(timeout)

        return [(key.fd, event) for key, event in EventedSocketIO.selector.select(timeout)]

    @staticmethod
    def add_listener(interface, bind_address, socket_type=socket.AF_INET):
        EventedSocketIO.ensure_backend()
        if socket_type == socket.AF_INET:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if RNS.vendor.platformutils.is_windows():
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            else:
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(bind_address)
        elif socket_type == socket.AF_INET6:
            server_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            if RNS.vendor.platformutils.is_windows():
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            else:
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(bind_address)
        elif socket_type == socket.AF_UNIX:
            server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server_socket.bind(bind_address)
        else:
            raise TypeError(f"Invalid socket type {socket_type} for {interface}")

        server_socket.listen(1)
        server_socket.setblocking(0)
        EventedSocketIO.listener_filenos[server_socket.fileno()] = (interface, server_socket)
        EventedSocketIO._register_or_recover_fileno(server_socket.fileno(), EventedSocketIO._read_mask(), interface)
        EventedSocketIO.start()

    @staticmethod
    def add_client_socket(client_socket, interface):
        EventedSocketIO.ensure_backend()
        client_socket.setblocking(0)
        EventedSocketIO.spawned_interface_filenos[client_socket.fileno()] = interface
        EventedSocketIO.register_in(client_socket.fileno())
        EventedSocketIO.start()

    @staticmethod
    def register_in(fileno):
        if fileno < 0:
            RNS.log(f"Attempt to register invalid file descriptor {fileno}", RNS.LOG_WARNING)
            return

        try:
            if not EventedSocketIO._register_or_recover_fileno(fileno, EventedSocketIO._read_mask()):
                RNS.log(f"Unable to register local I/O read interest for file descriptor {fileno}", RNS.LOG_WARNING)
        except Exception as e:
            RNS.log(f"An error occurred while registering local I/O read interest for file descriptor {fileno}: {e}", RNS.LOG_WARNING)

    @staticmethod
    def deregister_fileno(fileno):
        if fileno < 0:
            RNS.log(f"Attempt to deregister invalid file descriptor {fileno}", RNS.LOG_DEBUG)
            return

        try:
            EventedSocketIO._unregister_fileno(fileno)
        except Exception as e:
            RNS.log(f"An error occurred while deregistering file descriptor {fileno}: {e}", RNS.LOG_DEBUG)

    @staticmethod
    def deregister_listeners():
        for fileno in list(EventedSocketIO.listener_filenos):
            owner_interface, server_socket = EventedSocketIO.listener_filenos[fileno]
            fileno = server_socket.fileno()
            EventedSocketIO.deregister_fileno(fileno)
            server_socket.close()

        EventedSocketIO.listener_filenos.clear()

    @staticmethod
    def tx_ready(interface):
        if interface.socket:
            fileno = interface.socket.fileno()
            if fileno < 0:
                EventedSocketIO._remove_spawned_interface_by_object(interface)
                return
            if fileno in EventedSocketIO.spawned_interface_filenos:
                EventedSocketIO._note_tx_buffer(len(interface.transmit_buffer))
                try:
                    recovered = EventedSocketIO._modify_or_recover_fileno(fileno, EventedSocketIO._read_write_mask(), interface)
                    if not recovered:
                        EventedSocketIO._close_client_socket(fileno, interface, interface.socket, error=True)
                except Exception as e:
                    RNS.log(f"Error occurred on {interface} while modifying local I/O state: {e}", RNS.LOG_WARNING)
                    EventedSocketIO._close_client_socket(fileno, interface, interface.socket, error=True)

    @staticmethod
    def _set_client_interest(fileno, interface):
        if len(interface.transmit_buffer) > 0:
            mask = EventedSocketIO._read_write_mask()
        else:
            mask = EventedSocketIO._read_mask()

        return EventedSocketIO._modify_or_recover_fileno(fileno, mask, interface)

    @staticmethod
    def _remove_spawned_interface(fileno, spawned_interface):
        try:
            if fileno in EventedSocketIO.spawned_interface_filenos:
                EventedSocketIO.spawned_interface_filenos.pop(fileno)
        except Exception as e:
            RNS.log(f"Error while removing spawned interface file descriptor from local I/O handler: {e}", RNS.LOG_ERROR)

        pif = None
        try:
            if spawned_interface.parent_interface:
                pif = spawned_interface.parent_interface
                if hasattr(pif, "spawned_interfaces") and pif.spawned_interfaces != None:
                    while spawned_interface in pif.spawned_interfaces:
                        pif.spawned_interfaces.remove(spawned_interface)
        except Exception as e:
            RNS.log(f"Error while removing spawned interface from {pif}: {e}", RNS.LOG_ERROR)

    @staticmethod
    def _remove_spawned_interface_by_object(spawned_interface):
        for fileno, mapped_interface in list(EventedSocketIO.spawned_interface_filenos.items()):
            if mapped_interface == spawned_interface:
                EventedSocketIO.deregister_fileno(fileno)
                EventedSocketIO._remove_spawned_interface(fileno, spawned_interface)

    @staticmethod
    def _close_client_socket(fileno, spawned_interface, client_socket, error=False):
        if error:
            EventedSocketIO._note_stat("socket_error_count")
        else:
            EventedSocketIO._note_stat("socket_close_count")

        EventedSocketIO.deregister_fileno(fileno)
        EventedSocketIO._remove_spawned_interface(fileno, spawned_interface)

        try:
            client_socket.close()
        except Exception as e:
            RNS.log(f"Error while closing socket for {spawned_interface}: {e}", RNS.LOG_WARNING)

        try:
            spawned_interface.receive(b"")
        except Exception as e:
            RNS.log(f"Error while notifying {spawned_interface} of socket close: {e}", RNS.LOG_DEBUG)

    @staticmethod
    def _read_client_socket(fileno, spawned_interface, client_socket):
        read_total = 0
        closed = False

        EventedSocketIO._note_stat("read_events")
        while read_total < QORTAL_RNS_EVENTED_IO_READ_BUDGET_BYTES:
            read_size = min(spawned_interface.HW_MTU, QORTAL_RNS_EVENTED_IO_READ_BUDGET_BYTES - read_total)
            try:
                received_bytes = client_socket.recv(read_size)
            except (BlockingIOError, InterruptedError):
                break
            except Exception as e:
                RNS.log(f"Error while reading from {spawned_interface}: {e}", RNS.LOG_DEBUG)
                EventedSocketIO._close_client_socket(fileno, spawned_interface, client_socket, error=True)
                closed = True
                break

            if len(received_bytes) == 0:
                EventedSocketIO._close_client_socket(fileno, spawned_interface, client_socket)
                closed = True
                break

            read_total += len(received_bytes)
            spawned_interface.receive(received_bytes)

        if read_total > 0:
            EventedSocketIO._note_stat("read_bytes", read_total)

        if not closed and read_total >= QORTAL_RNS_EVENTED_IO_READ_BUDGET_BYTES:
            EventedSocketIO._note_stat("read_budget_hits")

        return closed

    @staticmethod
    def _write_client_socket(fileno, spawned_interface, client_socket):
        written_total = 0
        closed = False

        EventedSocketIO._note_stat("write_events")
        while len(spawned_interface.transmit_buffer) > 0 and written_total < QORTAL_RNS_EVENTED_IO_WRITE_BUDGET_BYTES:
            remaining_budget = QORTAL_RNS_EVENTED_IO_WRITE_BUDGET_BYTES - written_total
            chunk = spawned_interface.transmit_buffer[:remaining_budget]
            try:
                written = client_socket.send(chunk)
            except (BlockingIOError, InterruptedError):
                break
            except Exception as e:
                if not spawned_interface.detached:
                    RNS.log(f"Error while writing to {spawned_interface}: {e}", RNS.LOG_DEBUG)
                EventedSocketIO._close_client_socket(fileno, spawned_interface, client_socket, error=True)
                closed = True
                break

            if written <= 0:
                EventedSocketIO._close_client_socket(fileno, spawned_interface, client_socket, error=True)
                closed = True
                break

            written_total += written
            spawned_interface.transmit_buffer = spawned_interface.transmit_buffer[written:]
            spawned_interface.txb += written
            if spawned_interface.parent_interface:
                spawned_interface.parent_interface.txb += written

        if written_total > 0:
            EventedSocketIO._note_stat("write_bytes", written_total)

        if not closed and len(spawned_interface.transmit_buffer) > 0:
            EventedSocketIO._note_tx_buffer(len(spawned_interface.transmit_buffer))
            if written_total >= QORTAL_RNS_EVENTED_IO_WRITE_BUDGET_BYTES:
                EventedSocketIO._note_stat("write_budget_hits")

        if not closed:
            try:
                if not EventedSocketIO._set_client_interest(fileno, spawned_interface):
                    EventedSocketIO._close_client_socket(fileno, spawned_interface, client_socket, error=True)
                    closed = True
            except Exception as e:
                RNS.log(f"Error while setting local event I/O interest on {spawned_interface}: {e}", RNS.LOG_ERROR)
                EventedSocketIO._close_client_socket(fileno, spawned_interface, client_socket, error=True)
                closed = True

        return closed

    @staticmethod
    def __job():
        try:
            EventedSocketIO.ensure_backend()
            while True:
                events = EventedSocketIO._poll(1)
                EventedSocketIO._maybe_log_stats()
                for fileno, event in events:
                    if fileno in EventedSocketIO.spawned_interface_filenos:
                        spawned_interface = EventedSocketIO.spawned_interface_filenos[fileno]
                        client_socket = spawned_interface.socket
                        socket_closed = False

                        if not socket_closed and client_socket and fileno == client_socket.fileno() and EventedSocketIO._is_read_event(event):
                            socket_closed = EventedSocketIO._read_client_socket(fileno, spawned_interface, client_socket)

                        if not socket_closed and client_socket and fileno == client_socket.fileno() and EventedSocketIO._is_write_event(event):
                            socket_closed = EventedSocketIO._write_client_socket(fileno, spawned_interface, client_socket)

                        if not socket_closed and client_socket and fileno == client_socket.fileno() and (event & EventedSocketIO._hup_mask()):
                            EventedSocketIO._close_client_socket(fileno, spawned_interface, client_socket, error=bool(event & select.EPOLLERR))

                    elif fileno in EventedSocketIO.listener_filenos:
                        owner_interface, server_socket = EventedSocketIO.listener_filenos[fileno]
                        if fileno == server_socket.fileno() and EventedSocketIO._is_read_event(event):
                            client_socket = None
                            try:
                                client_socket, address = server_socket.accept()
                                client_socket.setblocking(0)
                                if not owner_interface.incoming_connection(client_socket):
                                    try:
                                        client_socket.close()
                                    except Exception as e:
                                        RNS.log(f"Error while closing socket for failed incoming connection: {e}", RNS.LOG_WARNING)
                            except Exception as e:
                                RNS.log(f"Accepting socket failed for incoming connection: {e}", RNS.LOG_WARNING)
                                if client_socket != None:
                                    try:
                                        client_socket.close()
                                    except Exception as close_error:
                                        RNS.log(f"Error while closing socket for failed incoming socket accept: {close_error}", RNS.LOG_WARNING)

                        if fileno == server_socket.fileno() and (event & EventedSocketIO._hup_mask()):
                            try:
                                EventedSocketIO.deregister_fileno(fileno)
                            except Exception as e:
                                RNS.log(f"Error while deregistering listener file descriptor {fileno}: {e}", RNS.LOG_ERROR)

                            try:
                                server_socket.close()
                            except Exception as e:
                                RNS.log(f"Error while closing listener socket for {server_socket}: {e}", RNS.LOG_WARNING)

        except Exception as e:
            RNS.log(f"EventedSocketIO error: {e}", RNS.LOG_ERROR)
            RNS.trace_exception(e)

        finally:
            EventedSocketIO.deregister_listeners()
            with EventedSocketIO._job_lock:
                EventedSocketIO._job_active = False
