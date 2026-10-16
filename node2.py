"""P2PNode: assembles the mixins into one peer."""
import os
import socket
import threading

from config import HOST
from registry import RegistryMixin
from network import NetworkMixin
from uploader import UploadMixin
from downloader import DownloadMixin


class P2PNode(RegistryMixin, NetworkMixin, UploadMixin, DownloadMixin):
    def __init__(self, port, bootstrap=None):
        self.port = int(port)
        self.address = f"{HOST}:{self.port}"
        self.bootstrap = bootstrap
        self.root = os.path.abspath(f"peer_{self.port}_files")
        self.download_dir = os.path.join(self.root, "downloads")
        self.metadata_dir = os.path.join(self.root, "metadata")
        self.progress_dir = os.path.join(self.root, "progress")
        for path in (self.root, self.download_dir, self.metadata_dir, self.progress_dir):
            os.makedirs(path, exist_ok=True)

        self.catalog_path = os.path.join(self.root, "uploaded_files.json")
        self.catalog = self._load_json_file(self.catalog_path, {})

        # file_id -> {"metadata": metadata, "original_uploader": addr, "seeders": set(addr)}
        # "seeders" means: peers holding AT LEAST ONE verified piece.
        self.files = {}
        self.files_lock = threading.Lock()

        # Which pieces THIS peer holds, and where they are stored on disk.
        self.have = {}      # file_id -> set(piece indexes)
        self.storage = {}   # file_id -> path of complete file or .part file
        self.have_lock = threading.Lock()

        self.peers = set()
        self.peers_lock = threading.Lock()
        self.stop_event = threading.Event()

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((HOST, self.port))
        self.server.listen(50)
        self.server.settimeout(1.0)

        # Load local state BEFORE starting network threads so the first HELLO
        # already advertises files this peer holds.
        self._load_local_state()

        threading.Thread(target=self._accept_loop, daemon=True).start()
        threading.Thread(target=self._gossip_loop, daemon=True).start()

        if bootstrap and bootstrap != self.address:
            self._join_network(bootstrap)

        print(f"[{self.address}] Ready. Files folder: {self.root}")
        print("Commands: help, peers, upload <path>, files, search <filename>, download <filename>, quit")

    def close(self):
        self.stop_event.set()
        try:
            self.server.close()
        except OSError:
            pass
