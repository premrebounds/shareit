"""Networking: server loop, request handling (HELLO/GET_METADATA/GET_BITFIELD/GET_CHUNK), gossip."""
import socket
import threading
import time

from config import SOCKET_TIMEOUT, UPLOAD_DELAY
from protocol import send_json, recv_json


class NetworkMixin:
    """Peer-to-peer messaging and gossip."""

    def _connect_json(self, peer, request, timeout=SOCKET_TIMEOUT):
        ip, port = peer.rsplit(":", 1)
        with socket.create_connection((ip, int(port)), timeout=timeout) as sock:
            sock.settimeout(timeout)
            send_json(sock, request)
            return recv_json(sock)

    def _join_network(self, peer):
        try:
            response = self._connect_json(peer, {
                "type": "HELLO",
                "from": self.address,
                "peers": self._peer_list(),
                "files": self._shareable_summaries(),
            })
            with self.peers_lock:
                self.peers.add(peer)
                for p in response.get("peers", []):
                    if p != self.address:
                        self.peers.add(p)
            for summary in response.get("files", []):
                self._register_summary(summary)
            print(f"[{self.address}] Joined network through {peer}")
        except Exception as e:
            print(f"[{self.address}] Could not contact bootstrap {peer}: {e}")

    def _accept_loop(self):
        while not self.stop_event.is_set():
            try:
                conn, _addr = self.server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle_connection, args=(conn,), daemon=True).start()

    def _handle_connection(self, conn):
        with conn:
            conn.settimeout(SOCKET_TIMEOUT)
            try:
                req = recv_json(conn)
                kind = req.get("type")

                if kind == "HELLO":
                    sender = req.get("from")
                    if sender and sender != self.address:
                        with self.peers_lock:
                            self.peers.add(sender)
                            for p in req.get("peers", []):
                                if p != self.address:
                                    self.peers.add(p)
                    for summary in req.get("files", []):
                        self._register_summary(summary)
                    send_json(conn, {"ok": True, "peers": self._peer_list(), "files": self._shareable_summaries()})

                elif kind == "GET_METADATA":
                    file_id = req.get("file_id")
                    with self.files_lock:
                        rec = self.files.get(file_id)
                        metadata = rec["metadata"] if rec and "piece_hashes" in rec["metadata"] else None
                    if metadata:
                        send_json(conn, {"ok": True, "metadata": metadata})
                    else:
                        send_json(conn, {"ok": False, "error": "metadata not found"})

                elif kind == "GET_BITFIELD":
                    file_id = req.get("file_id")
                    with self.have_lock:
                        pieces = sorted(self.have.get(file_id, ()))
                    send_json(conn, {"ok": True, "pieces": pieces})

                elif kind == "GET_CHUNK":
                    file_id = req.get("file_id")
                    index = int(req.get("index", -1))
                    with self.files_lock:
                        rec = self.files.get(file_id)
                        metadata = rec["metadata"] if rec else None
                    if not metadata or "piece_hashes" not in metadata:
                        send_json(conn, {"ok": False, "error": "file not available"})
                        return
                    piece_size = metadata["piece_size"]
                    offset = index * piece_size
                    if index < 0 or offset >= metadata["file_size"]:
                        send_json(conn, {"ok": False, "error": "invalid piece index"})
                        return
                    length = min(piece_size, metadata["file_size"] - offset)

                    data = None
                    for _ in range(2):  # retry once in case the .part file was just renamed
                        with self.have_lock:
                            has = index in self.have.get(file_id, ())
                            path = self.storage.get(file_id)
                        if not has or not path:
                            break
                        try:
                            with open(path, "rb") as f:
                                f.seek(offset)
                                data = f.read(length)
                            break
                        except FileNotFoundError:
                            time.sleep(0.05)
                    if data is None or len(data) != length:
                        send_json(conn, {"ok": False, "error": "piece unavailable"})
                        return
                    if UPLOAD_DELAY:
                        time.sleep(UPLOAD_DELAY)   # simulate limited upload bandwidth
                    send_json(conn, {"ok": True, "length": length})
                    conn.sendall(data)

                else:
                    send_json(conn, {"ok": False, "error": "unknown request"})
            except Exception as e:
                try:
                    send_json(conn, {"ok": False, "error": str(e)})
                except Exception:
                    pass

    def _gossip_once(self, timeout=4):
        peers = self._peer_list()
        for peer in peers:
            if peer == self.address:
                continue
            try:
                response = self._connect_json(peer, {
                    "type": "HELLO",
                    "from": self.address,
                    "peers": peers,
                    "files": self._shareable_summaries(),
                }, timeout=timeout)
                with self.peers_lock:
                    for p in response.get("peers", []):
                        if p != self.address:
                            self.peers.add(p)
                for summary in response.get("files", []):
                    self._register_summary(summary)
            except Exception:
                # A stale peer is kept for now; future attempts may succeed.
                continue

    def _gossip_loop(self):
        while not self.stop_event.wait(5):
            self._gossip_once()
