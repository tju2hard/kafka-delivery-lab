"""Kafka wire faults, with a dedicated advertised FAULT listener per broker.

Drop a successful ProduceResponse, or retain an already received ProduceRequest
after its originating process is killed. Neither changes a message or broker.
"""
import json
import socket
import struct
import threading
import time


def exact(sock, count):
    chunks = bytearray()
    while len(chunks) < count:
        data = sock.recv(count - len(chunks))
        if not data:
            raise EOFError()
        chunks.extend(data)
    return bytes(chunks)


def frame(sock):
    header = exact(sock, 4)
    length = struct.unpack('>i', header)[0]
    if not 0 < length <= 16 * 1024 * 1024:
        raise ValueError('Invalid Kafka frame size')
    return header + exact(sock, length)


def varint(data, pos):
    value, shift = 0, 0
    while True:
        byte = data[pos]
        pos += 1
        value |= (byte & 127) << shift
        if not byte & 128:
            return value, pos
        shift += 7
        if shift > 35:
            raise ValueError('Invalid unsigned varint')


def tags(data, pos):
    count, pos = varint(data, pos)
    for _ in range(count):
        _, pos = varint(data, pos)
        size, pos = varint(data, pos)
        pos += size
    return pos


def produce_error(response, version):
    """Decode the first partition status; only drop a confirmed successful ACK."""
    data, pos = response[4:], 4  # response correlation ID
    if version >= 9:
        pos = tags(data, pos)
        count, pos = varint(data, pos)
        if count != 2:
            raise ValueError('Expected one topic in lab ProduceResponse')
        length, pos = varint(data, pos)
        pos += length - 1
        count, pos = varint(data, pos)
        if count != 2:
            raise ValueError('Expected one partition in lab ProduceResponse')
    else:
        count = struct.unpack_from('>i', data, pos)[0]
        pos += 4
        if count != 1:
            raise ValueError('Expected one topic')
        length = struct.unpack_from('>h', data, pos)[0]
        pos += 2 + length
        count = struct.unpack_from('>i', data, pos)[0]
        pos += 4
        if count != 1:
            raise ValueError('Expected one partition')
    pos += 4  # partition index
    return struct.unpack_from('>h', data, pos)[0]


class WireProxy:
    def __init__(self):
        self.mode = 'pass'
        self.triggered = False
        self.held = threading.Event()
        self.release = threading.Event()
        self.ack = threading.Event()
        self.lock = threading.Lock()
        self.sockets = []
        self.witnesses = []
        self.errors = []
        self.listeners = []
        self.stopped = False

    def start(self):
        for broker in (1, 2, 3):
            listener = socket.socket()
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(('0.0.0.0', 19090 + broker))
            listener.listen()
            self.listeners.append(listener)
            threading.Thread(target=self.accept, args=(listener, broker), daemon=True).start()

    def configure(self, mode):
        self.mode, self.triggered = mode, False
        self.held.clear()
        self.release.clear()
        self.ack.clear()
        self.witnesses = []
        self.errors = []

    def accept(self, listener, broker):
        while not self.stopped:
            try:
                client, _ = listener.accept()
                server = socket.create_connection((f'kafka{broker}', 9094), 10)
                server.settimeout(120)
                self.sockets.extend([client, server])
                threading.Thread(target=self.bridge, args=(client, server), daemon=True).start()
            except OSError:
                if not self.stopped:
                    self.errors.append('proxy accept/connect failed')

    def bridge(self, client, server):
        pending = {}

        def responses():
            try:
                while not self.stopped:
                    data = frame(server)
                    correlation = struct.unpack_from('>i', data, 4)[0]
                    api, version = pending.pop(correlation)
                    if api == 0:
                        error = produce_error(data, version)
                        if error == 0:
                            self.ack.set()
                            with self.lock:
                                drop = self.mode == 'drop_ack' and not self.triggered
                                if drop:
                                    self.triggered = True
                                    self.witnesses.append({'fault': 'successful_produce_response_dropped', 'at_ns': time.monotonic_ns(), 'error_code': error, 'api_version': version})
                            if drop:
                                client.shutdown(socket.SHUT_RDWR)
                                client.close()
                                server.close()
                                return
                    client.sendall(data)
            except (EOFError, OSError):
                pass
            except Exception as error:
                self.errors.append(type(error).__name__ + ': ' + str(error))

        threading.Thread(target=responses, daemon=True).start()
        try:
            while not self.stopped:
                data = frame(client)
                api, version, correlation = struct.unpack_from('>hhi', data, 4)
                pending[correlation] = (api, version)
                with self.lock:
                    hold = api == 0 and self.mode == 'hold_request' and not self.triggered
                    if hold:
                        self.triggered = True
                if hold:
                    self.witnesses.append({'fault': 'produce_request_received_but_not_forwarded', 'at_ns': time.monotonic_ns()})
                    self.held.set()
                    if not self.release.wait(100):
                        raise TimeoutError('Fault controller did not release delayed request')
                    self.witnesses.append({'fault': 'old_request_forwarded_after_new_writer', 'at_ns': time.monotonic_ns()})
                server.sendall(data)
        except (EOFError, OSError):
            pass
        except Exception as error:
            self.errors.append(type(error).__name__ + ': ' + str(error))

    def close(self):
        self.stopped = True
        self.release.set()
        for sock in self.listeners + self.sockets:
            try:
                sock.close()
            except OSError:
                pass
