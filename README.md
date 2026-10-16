# shareit
# Small P2P file sharing (BitTorrent-style demo)

Peers share files in pieces. Any peer holding at least one verified piece serves it.
Downloads fetch the rarest piece first, from the least-used peer that has it.

## Layout
| File | Role |
|---|---|
| `config.py` | Tunable constants (piece size limits, upload delay, timeouts) |
| `protocol.py` | Length-prefixed JSON, hashing, safe filenames, adaptive piece size |
| `registry.py` | Known files, persistence of metadata |
| `network.py` | Server loop, request handlers, gossip |
| `uploader.py` | Upload, search, list |
| `downloader.py` | Rarest-first multi-source downloader |
| `node.py` | `P2PNode`, assembles the mixins |
| `p2p.py` | CLI entry point |

## Run
```
python p2p.py --port 5000
python p2p.py --port 5001 --bootstrap 127.0.0.1:5000
python p2p.py --port 5002 --bootstrap 127.0.0.1:5000
python p2p.py --port 5003 --bootstrap 127.0.0.1:5000
```
Demo: `upload demo.bin` on 5000, `partial demo.bin 40` on 5001 and 5002, `download demo.bin` on 5003.
