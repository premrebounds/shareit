"""Local state: known files, which pieces we hold, persistence of metadata."""
import os
import json

from protocol import safe_filename


class RegistryMixin:
    """File registry, JSON persistence and peer list."""

    @staticmethod
    def _load_json_file(path, fallback):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return fallback

    def _save_json_file(self, path, data):
        temp = path + ".tmp"
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(temp, path)

    def _load_local_state(self):
        """Re-register every complete file (uploaded or downloaded) that has saved metadata."""
        try:
            names = os.listdir(self.metadata_dir)
        except OSError:
            return
        for name in names:
            if not name.endswith(".torrent"):
                continue
            try:
                with open(os.path.join(self.metadata_dir, name), "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                filename = safe_filename(metadata["filename"])
                file_id = metadata["file_id"]
                for path in (os.path.join(self.root, filename), os.path.join(self.download_dir, filename)):
                    if os.path.isfile(path) and os.path.getsize(path) == metadata["file_size"]:
                        self._register_file(metadata, metadata.get("original_uploader"), self.address)
                        with self.have_lock:
                            self.storage[file_id] = path
                            self.have[file_id] = set(range(metadata["piece_count"]))
                        break
            except (OSError, json.JSONDecodeError, KeyError, ValueError):
                continue

    def _register_file(self, metadata, original_uploader, seeder=None):
        file_id = metadata["file_id"]
        who = seeder or original_uploader
        with self.files_lock:
            current = self.files.get(file_id)
            if current:
                # Upgrade a hash-less discovery stub to full metadata.
                if "piece_hashes" not in current["metadata"] and "piece_hashes" in metadata:
                    current["metadata"] = metadata
                if who:
                    current["seeders"].add(who)
            else:
                self.files[file_id] = {
                    "metadata": metadata,
                    "original_uploader": original_uploader,
                    "seeders": {who} if who else set(),
                }

    def _register_summary(self, summary):
        file_id = summary.get("file_id")
        if not file_id:
            return
        with self.files_lock:
            rec = self.files.get(file_id)
            if rec:
                rec["seeders"].update(summary.get("seeders", []))
            else:
                # Summary has no hashes; it is only a discovery hint.
                self.files[file_id] = {
                    "metadata": {
                        "file_id": file_id,
                        "filename": summary.get("filename", "unknown"),
                    },
                    "original_uploader": summary.get("original_uploader"),
                    "seeders": set(summary.get("seeders", [])),
                }

    def _shareable_summaries(self):
        summaries = []
        with self.files_lock:
            for file_id, rec in self.files.items():
                summaries.append({
                    "file_id": file_id,
                    "filename": rec["metadata"]["filename"],
                    "original_uploader": rec["original_uploader"],
                    "seeders": sorted(rec["seeders"]),
                })
        return summaries

    def _peer_list(self):
        with self.peers_lock:
            return sorted(self.peers)
