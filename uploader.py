"""Uploading files (hashing, adaptive piece size) plus search/listing."""
import os
import time
import shutil
import hashlib
import threading

from protocol import safe_filename, sha256_bytes, choose_piece_size


class UploadMixin:
    """Create metadata for a local file and seed it; search and list files."""

    def upload_file(self, source_path):
        source_path = os.path.abspath(os.path.expanduser(source_path.strip('"')))
        if not os.path.isfile(source_path):
            print("File not found:", source_path)
            return
        filename = safe_filename(os.path.basename(source_path))
        destination = os.path.join(self.root, filename)
        if os.path.abspath(source_path) != os.path.abspath(destination):
            shutil.copy2(source_path, destination)

        file_size = os.path.getsize(destination)
        piece_size = choose_piece_size(file_size)
        piece_hashes = []
        whole_hash = hashlib.sha256()
        with open(destination, "rb") as f:
            while True:
                piece = f.read(piece_size)
                if not piece:
                    break
                piece_hashes.append(sha256_bytes(piece))
                whole_hash.update(piece)
        file_id = whole_hash.hexdigest()
        metadata = {
            "file_id": file_id,
            "filename": filename,
            "file_size": file_size,
            "piece_size": piece_size,
            "piece_count": len(piece_hashes),
            "piece_hashes": piece_hashes,
            "original_uploader": self.address,
        }
        metadata_path = os.path.join(self.metadata_dir, file_id + ".torrent")
        self._save_json_file(metadata_path, metadata)

        self.catalog[file_id] = {
            "filename": filename,
            "file_id": file_id,
            "metadata_path": metadata_path,
            "original_uploader": self.address,
            "uploaded_at": time.time(),
        }
        self._save_json_file(self.catalog_path, self.catalog)

        self._register_file(metadata, self.address, self.address)
        # A re-upload may use a different piece size than a stale entry loaded from disk
        # at startup, so always replace the in-memory metadata with the fresh one.
        with self.files_lock:
            self.files[file_id]["metadata"] = metadata
        with self.have_lock:
            self.storage[file_id] = destination
            self.have[file_id] = set(range(len(piece_hashes)))

        print(f"Uploaded and seeded: {filename}")
        print(f"File ID (SHA-256): {file_id}")
        print(f"Pieces: {len(piece_hashes)} | Piece size: {piece_size} bytes")
        print(f"Metadata: {metadata_path}")
        threading.Thread(target=self._gossip_once, daemon=True).start()

    def _find_file_record(self, filename):
        matches = []
        with self.files_lock:
            for file_id, rec in self.files.items():
                meta = rec["metadata"]
                if meta.get("filename", "").lower() == filename.lower():
                    matches.append((file_id, {
                        "metadata": rec["metadata"],
                        "original_uploader": rec["original_uploader"],
                        "seeders": set(rec["seeders"]),
                    }))
        return matches

    def search(self, filename):
        matches = self._find_file_record(filename)
        if not matches:
            print(f"No known file named {filename!r}. Wait a few seconds for gossip or check peer connectivity.")
            return
        for file_id, rec in matches:
            meta = rec["metadata"]
            print(f"- {meta.get('filename')} | id={file_id[:16]}... | original={rec.get('original_uploader')} | "
                  f"peers with pieces={', '.join(sorted(rec['seeders'])) or 'none'}")

    def list_files(self):
        print("Known network files:")
        with self.files_lock:
            records = list(self.files.items())
        if not records:
            print("  (none yet)")
            return
        for file_id, rec in records:
            meta = rec["metadata"]
            with self.have_lock:
                mine = len(self.have.get(file_id, ()))
            total = meta.get("piece_count")
            mine_txt = f" | mine={mine}/{total}" if total else ""
            print(f"  {meta.get('filename', 'unknown')} | {file_id[:16]}... | original={rec.get('original_uploader')} | "
                  f"peers with pieces={', '.join(sorted(rec['seeders']))}{mine_txt}")
