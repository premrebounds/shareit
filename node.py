import os
import socket
import threading
import json
import time
from utils import generate_manifest, get_specific_chunk, verify_chunk_data, reassemble_file

class P2PNode:
    def __init__(self, host, port, shared_dir):
        self.host = host
        self.port = port
        self.shared_dir = shared_dir
        self.peers = []  # List of tuples: (host, port)
        self.running = True
        
        # Ensure shared directory exists
        if not os.path.exists(self.shared_dir):
            os.makedirs(self.shared_dir)

        # Start background TCP server listener
        self.server_thread = threading.Thread(target=self._start_server, daemon=True)
        self.server_thread.start()

    def add_peer(self, host, port):
        if (host, port) != (self.host, self.port) and (host, port) not in self.peers:
            self.peers.append((host, port))
            print(f"[{self.port}] Connected to peer -> {host}:{port}")

    def _start_server(self):
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((self.host, self.port))
        server_sock.listen(5)
        
        while self.running:
            try:
                client_sock, addr = server_sock.accept()
                threading.Thread(target=self._handle_client_connection, args=(client_sock,), daemon=True).start()
            except Exception:
                break

    def _handle_client_connection(self, client_sock):
        try:
            data = client_sock.recv(4096)
            if not data:
                return
            request = json.loads(data.decode('utf-8'))
            action = request.get("action")

            if action == "SEARCH_FILE":
                filename = request.get("filename")
                file_path = os.path.join(self.shared_dir, filename)
                if os.path.exists(file_path):
                    manifest = generate_manifest(file_path)
                    response = {"status": "FOUND", "manifest": manifest}
                else:
                    response = {"status": "NOT_FOUND"}
                client_sock.sendall(json.dumps(response).encode('utf-8'))

            elif action == "GET_CHUNK":
                filename = request.get("filename")
                chunk_index = request.get("index")
                file_path = os.path.join(self.shared_dir, filename)
                
                if os.path.exists(file_path):
                    chunk_data = get_specific_chunk(file_path, chunk_index)
                    client_sock.sendall(chunk_data)
                else:
                    client_sock.sendall(b"")
        except Exception as e:
            pass
        finally:
            client_sock.close()

    def search_file_network(self, filename):
        """
        Broadcasts a SEARCH_FILE query to all known peers to discover who has the file.
        """
        for peer_host, peer_port in self.peers:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2.0)
                s.connect((peer_host, peer_port))
                request = {"action": "SEARCH_FILE", "filename": filename}
                s.sendall(json.dumps(request).encode('utf-8'))
                
                response_data = s.recv(8192)
                s.close()
                
                if response_data:
                    res = json.loads(response_data.decode('utf-8'))
                    if res.get("status") == "FOUND":
                        print(f"[{self.port}] Found file manifest from peer {peer_host}:{peer_port}")
                        return res.get("manifest"), (peer_host, peer_port)
            except Exception:
                print(f"[{self.port}] Peer {peer_host}:{peer_port} is offline during search.")
        return None, None

    def download_chunk_from_peer(self, peer_host, peer_port, filename, chunk_index, expected_hash, received_chunks, lock):
        """
        Fetches a specific chunk index from a peer, verifies its SHA-256 hash, and saves it.
        Handles dropouts and socket exceptions gracefully.
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3.0)
            s.connect((peer_host, peer_port))
            
            request = {"action": "GET_CHUNK", "filename": filename, "index": chunk_index}
            s.sendall(json.dumps(request).encode('utf-8'))
            
            chunk_data = b""
            while True:
                packet = s.recv(1024)
                if not packet:
                    break
                chunk_data += packet
            s.close()

            if chunk_data and verify_chunk_data(chunk_data, expected_hash):
                with lock:
                    received_chunks[chunk_index] = chunk_data
                print(f"[{self.port}] Successfully downloaded & verified chunk {chunk_index} from {peer_port}")
                return True
            else:
                print(f"[{self.port}] Checksum mismatch or empty data for chunk {chunk_index} from {peer_port}")
        except Exception:
            print(f"[{self.port}] Connection dropped/failed with peer {peer_host}:{peer_port} while fetching chunk {chunk_index}")
            if (peer_host, peer_port) in self.peers:
                self.peers.remove((peer_host, peer_port))  # Churn handling: remove dead peer
        return False

    def download_file(self, filename):
        """
        Coordinates concurrent downloading of different chunks across multiple active peers.
        """
        manifest, primary_source = self.search_file_network(filename)
        if not manifest:
            print(f"[{self.port}] File '{filename}' not found anywhere in the network.")
            return

        total_chunks = manifest["total_chunks"]
        chunks_meta = manifest["chunks"]
        received_chunks = {}
        lock = threading.Lock()
        threads = []

        print(f"[{self.port}] Starting concurrent download of '{filename}' ({total_chunks} chunks)...")

        # Distribute chunk requests across available peers concurrently
        for cm in chunks_meta:
            idx = cm["index"]
            hsh = cm["hash"]
            
            # Pick an available peer (round-robin style or primary source)
            target_peers = [primary_source] + self.peers
            # Deduplicate peers
            target_peers = list(dict.fromkeys(target_peers))
            
            if not target_peers:
                print(f"[{self.port}] No active peers available!")
                break

            # Assign chunks to threads across available peers
            chosen_peer = target_peers[idx % len(target_peers)]
            p_host, p_port = chosen_peer

            t = threading.Thread(
                target=self.download_chunk_from_peer,
                args=(p_host, p_port, filename, idx, hsh, received_chunks, lock)
            )
            threads.append(t)
            t.start()

        # Wait for all download threads to complete
        for t in threads:
            t.join()

        # Reassemble if all chunks are gathered
        if len(received_chunks) == total_chunks:
            output_path = os.path.join(self.shared_dir, filename)
            reassemble_file(output_path, received_chunks, total_chunks)
        else:
            print(f"[{self.port}] Download incomplete. Got {len(received_chunks)}/{total_chunks} chunks.")
