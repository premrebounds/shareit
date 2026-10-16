"""Downloading: rarest-first, load-balanced, multi-source piece scheduler."""
import os
import time
import socket
import random
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor

from config import MAX_WORKERS, SOCKET_TIMEOUT, STALL_TIMEOUT
from protocol import send_json, recv_json, recv_exact, sha256_bytes, safe_filename


class DownloadMixin:
    """Fetch a file from every peer that holds any of its pieces."""

    def _fetch_metadata(self, file_id, candidates):
        ordered = []
        for candidate in candidates:
            if candidate and candidate not in ordered:
                ordered.append(candidate)
        for peer in ordered:
            try:
                response = self._connect_json(peer, {"type": "GET_METADATA", "file_id": file_id})
                if response.get("ok") and response.get("metadata"):
                    return response["metadata"]
            except Exception:
                continue
        return None

    def _query_bitfield(self, peer, file_id):
        """Ask a peer which pieces of file_id it holds. Returns a set, or None if unreachable."""
        try:
            r = self._connect_json(peer, {"type": "GET_BITFIELD", "file_id": file_id}, timeout=4)
            if r.get("ok"):
                return set(int(i) for i in r.get("pieces", []))
        except Exception:
            pass
        return None

    def download_file(self, filename, workers=MAX_WORKERS, max_pieces=None):
        matches = self._find_file_record(filename)
        if not matches:
            print("File not found in the network catalog. Try again after peers gossip.")
            return
        if len(matches) > 1:
            print("Multiple files share that name. Use a unique filename or remove the duplicate from the test network.")
            self.search(filename)
            return

        file_id, rec = matches[0]
        original = rec.get("original_uploader")
        known = [p for p in ([original] + sorted(rec.get("seeders", set()))) if p and p != self.address]

        metadata = rec["metadata"] if "piece_hashes" in rec["metadata"] else self._fetch_metadata(file_id, known)
        if not metadata:
            print("Could not retrieve metadata from the original uploader or known seeders.")
            return
        if metadata.get("file_id") != file_id or "piece_hashes" not in metadata:
            print("Metadata does not match the requested file.")
            return

        count = metadata["piece_count"]
        piece_size = metadata["piece_size"]
        expected = (metadata["file_size"] + piece_size - 1) // piece_size
        if expected != count or len(metadata["piece_hashes"]) != count:
            print("Invalid metadata: piece count mismatch.")
            return

        # Upgrade any stub so this peer can answer GET_METADATA for others.
        self._register_file(metadata, original)

        target = os.path.join(self.download_dir, safe_filename(metadata["filename"]))
        temp = target + ".part"

        with self.have_lock:
            if len(self.have.get(file_id, ())) == count:
                print("You already have this file:", self.storage.get(file_id))
                return
            resuming = self.storage.get(file_id) == temp and os.path.isfile(temp)
            if not resuming:
                self.have[file_id] = set()
                self.storage[file_id] = temp
            held = set(self.have[file_id])
        if not resuming:
            with open(temp, "wb") as f:
                f.truncate(metadata["file_size"])

        # ---- shared download state ----
        state_lock = threading.Lock()
        needed = set(range(count)) - held
        inflight = set()
        availability = {}     # peer -> set(pieces it holds)
        inflight_peer = {}    # peer -> active requests
        received_from = {}    # peer -> pieces received during this download
        fails = {}            # (piece, peer) -> failure count
        piece_sources = {}    # piece -> peer it actually came from
        errors = []
        last_progress = [time.time()]
        announced = [bool(held)]
        stop = threading.Event()

        def refresh_sources():
            # Ask every peer we know of (not just announced seeders), so peers
            # that just got their first piece are found immediately.
            with self.files_lock:
                r = self.files.get(file_id)
                peers = set(r["seeders"]) if r else set()
            peers.update(self._peer_list())
            if original:
                peers.add(original)
            peers.discard(self.address)
            for peer in peers:
                bits = self._query_bitfield(peer, file_id)
                with state_lock:
                    if bits is not None:
                        availability[peer] = bits
                    else:
                        availability.pop(peer, None)   # unreachable: stop scheduling it

        def refresher():
            while not stop.wait(1.0):
                refresh_sources()

        def pick():
            """Rarest piece first, then the least-used peer that holds it."""
            with state_lock:
                if max_pieces is not None and len(piece_sources) + len(inflight) >= max_pieces:
                    return None
                options = []
                for i in needed - inflight:
                    holders = [p for p, bits in availability.items()
                               if i in bits and fails.get((i, p), 0) < 2]
                    if holders:
                        options.append((len(holders), random.random(), i, holders))
                if not options:
                    return None
                _, _, i, holders = min(options)
                peer = min(holders, key=lambda p: (inflight_peer.get(p, 0),
                                                   received_from.get(p, 0),
                                                   random.random()))
                inflight.add(i)
                inflight_peer[peer] = inflight_peer.get(peer, 0) + 1
                return i, peer

        def worker():
            while True:
                with state_lock:
                    if not needed:
                        return
                    if max_pieces is not None and len(piece_sources) >= max_pieces:
                        return
                if time.time() - last_progress[0] > STALL_TIMEOUT:
                    return
                job = pick()
                if not job:
                    time.sleep(0.2)
                    continue
                index, peer = job
                try:
                    length = min(piece_size, metadata["file_size"] - index * piece_size)
                    ip, port = peer.rsplit(":", 1)
                    with socket.create_connection((ip, int(port)), timeout=SOCKET_TIMEOUT) as sock:
                        sock.settimeout(SOCKET_TIMEOUT)
                        send_json(sock, {"type": "GET_CHUNK", "file_id": file_id, "index": index})
                        response = recv_json(sock)
                        if not response.get("ok") or response.get("length") != length:
                            raise IOError(response.get("error", "bad chunk response"))
                        data = recv_exact(sock, length)
                    if sha256_bytes(data) != metadata["piece_hashes"][index]:
                        raise ValueError("piece hash mismatch")

                    # Verified: write it, then start serving it to others.
                    with open(temp, "r+b") as out:
                        out.seek(index * piece_size)
                        out.write(data)
                        out.flush()
                    with self.have_lock:
                        self.have[file_id].add(index)

                    with state_lock:
                        needed.discard(index)
                        piece_sources[index] = peer
                        received_from[peer] = received_from.get(peer, 0) + 1
                        last_progress[0] = time.time()
                        first = not announced[0]
                        announced[0] = True
                    print(f"[download] piece {index + 1}/{count} VERIFIED from {peer}")

                    if first:  # become a seeder after the very first verified piece
                        self._register_file(metadata, original, self.address)
                        threading.Thread(target=self._gossip_once, daemon=True).start()
                except Exception as e:
                    with state_lock:
                        key = (index, peer)
                        fails[key] = fails.get(key, 0) + 1
                        errors.append(f"piece {index} from {peer}: {e}")
                        if fails[key] >= 2 and peer in availability:
                            availability[peer].discard(index)
                finally:
                    with state_lock:
                        inflight.discard(index)
                        inflight_peer[peer] = max(0, inflight_peer.get(peer, 1) - 1)

        refresh_sources()
        threading.Thread(target=refresher, daemon=True).start()
        n_workers = max(1, workers)
        print(f"Downloading {metadata['filename']} ({count} pieces) with {n_workers} worker(s)...")
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = [pool.submit(worker) for _ in range(n_workers)]
            for future in futures:
                future.result()
        stop.set()

        if needed and max_pieces is not None and len(piece_sources) >= max_pieces:
            with self.have_lock:
                mine = len(self.have[file_id])
            print(f"Partial download done: holding {mine}/{count} pieces and seeding them.")
            self._print_source_report(metadata["filename"], count, piece_sources)
            return

        if needed:
            print(f"Download incomplete: verified {count - len(needed)}/{count} pieces.")
            print("Partial file kept at:", temp)
            if errors:
                print("Last error:", errors[-1])
            self._print_source_report(metadata["filename"], count, piece_sources)
            return

        # Whole-file verification against the stable file ID.
        whole_hash = hashlib.sha256()
        with open(temp, "rb") as f:
            for block in iter(lambda: f.read(1024 * 1024), b""):
                whole_hash.update(block)
        if whole_hash.hexdigest() != file_id:
            print("Whole-file hash mismatch. Keeping .part file for inspection.")
            return

        with self.have_lock:
            os.replace(temp, target)
            self.storage[file_id] = target
        # Persist metadata so this file is re-seeded after a restart.
        self._save_json_file(os.path.join(self.metadata_dir, file_id + ".torrent"), metadata)

        print(f"Download complete and verified: {target}")
        self._print_source_report(metadata["filename"], count, piece_sources)

    def _print_source_report(self, filename, total_pieces, piece_sources):
        by_peer = {}
        for piece_index, peer in piece_sources.items():
            by_peer.setdefault(peer, []).append(piece_index + 1)

        print("\n========== DOWNLOAD SOURCE REPORT ==========")
        print(f"File: {filename}")
        print(f"Pieces fetched this session: {len(piece_sources)} (file has {total_pieces})")
        for peer in sorted(by_peer):
            indexes = sorted(by_peer[peer])
            print(f"Peer {peer}")
            print("  Pieces received: " + ", ".join(map(str, indexes)))
            print(f"  Total: {len(indexes)} piece(s)")
        print(f"Unique peers actually used: {len(by_peer)}")
        print("============================================\n")
