"""Wire protocol helpers: length-prefixed JSON, hashing, filename safety, piece sizing."""
import os
import json
import hashlib

from config import MIN_PIECE_SIZE, MAX_PIECE_SIZE, TARGET_PIECES


def recv_exact(sock, n):
    """Read exactly n bytes, or raise if the connection closes early."""
    chunks = []
    remaining = n
    while remaining:
        data = sock.recv(min(65536, remaining))
        if not data:
            raise ConnectionError("Connection closed before all bytes arrived")
        chunks.append(data)
        remaining -= len(data)
    return b"".join(chunks)


def send_json(sock, obj):
    raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    sock.sendall(len(raw).to_bytes(4, "big") + raw)


def recv_json(sock):
    size = int.from_bytes(recv_exact(sock, 4), "big")
    if size > 16 * 1024 * 1024:
        raise ValueError("JSON message too large")
    return json.loads(recv_exact(sock, size).decode("utf-8"))


def choose_piece_size(file_size):
    """Pick a power-of-two piece size so a file has roughly TARGET_PIECES pieces."""
    size = MIN_PIECE_SIZE
    while size < MAX_PIECE_SIZE and file_size / size > TARGET_PIECES:
        size *= 2
    return size


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def safe_filename(name):
    # Only allow a simple filename, never paths supplied by remote peers.
    clean = os.path.basename(name)
    if clean != name or clean in ("", ".", ".."):
        raise ValueError("Invalid filename")
    return clean
