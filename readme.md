# Terminal P2P File-Sharing System (BitTorrent Clone)

A lightweight, decentralized peer-to-peer file-sharing network built completely from scratch in Python. No heavy external frameworks or bloated libraries—just raw TCP sockets, multi-threading, cryptographic hashing, and a clean command-line interface.

Designed to handle multiple peers sharing, discovering, and concurrently downloading pieces of the same file simultaneously, complete with resilience against peer dropouts (churn) and an automated test suite.

---

## Project Architecture & Components

* **`utils.py`:** Handles file slicing into 1024-byte blocks, SHA-256 cryptographic manifest generation, chunk verification, and file reassembly.
* **`node.py`:** Implements the dual-role TCP server and client engine, network search protocol, and multi-threaded concurrent chunk downloading with exception handling for peer dropouts.
* **`main.py`:** Acts as the dynamic command-line entry point supporting arbitrary port assignments and bootstrap peer connections for scalable local multi-node networks.
* **`test_system.py`:** An automated core verification suite that tests file slicing, SHA-256 integrity checks, and sequential reassembly in an isolated sandbox environment.

---

## Project Structure

```text
├── utils.py        # File chunking, hashing, and reassembly logic
├── node.py         # Core P2PNode class, TCP server, and downloader
├── main.py         # Dynamic CLI entry point and interactive menu
├── test_system.py  # Automated core logic test suite
└── README.md       # Project documentation
