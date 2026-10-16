"""Command-line entry point:  python p2p.py --port 5000 [--bootstrap 127.0.0.1:5000]"""
import argparse

from config import MAX_WORKERS
from node import P2PNode


def main():
    parser = argparse.ArgumentParser(description="Small local P2P file-sharing demo")
    parser.add_argument("--port", type=int, required=True, help="Port for this peer, e.g. 5000")
    parser.add_argument("--bootstrap", default=None, help="Bootstrap peer, e.g. 127.0.0.1:5000")
    args = parser.parse_args()

    node = P2PNode(args.port, args.bootstrap)
    try:
        while True:
            try:
                command = input(f"[{node.port}]> ").strip()
            except EOFError:
                break
            if not command:
                continue
            lower = command.lower()
            if lower in ("quit", "exit"):
                break
            elif lower == "help":
                print("Commands:")
                print("  upload <path>      Copy a local file into this peer and create metadata")
                print("  files              List known network files")
                print("  peers              List known peers")
                print("  search <filename>  Search by exact filename")
                print("  download <filename> [workers]")
                print("  partial <filename> <n_pieces>   fetch only n pieces, then keep seeding them")
                print("  quit")
            elif lower == "peers":
                print("\n".join(node._peer_list()) or "(no peers known)")
            elif lower == "files":
                node.list_files()
            elif lower.startswith("upload "):
                node.upload_file(command[7:].strip())
            elif lower.startswith("search "):
                node.search(command[7:].strip())
            elif lower.startswith("partial "):
                parts = command.split()
                if len(parts) != 3 or not parts[2].isdigit():
                    print("Usage: partial <filename> <n_pieces>")
                else:
                    node.download_file(parts[1], max_pieces=int(parts[2]))
            elif lower.startswith("download "):
                parts = command.split()
                if len(parts) not in (2, 3):
                    print("Usage: download <filename> [workers]")
                else:
                    try:
                        workers = int(parts[2]) if len(parts) == 3 else MAX_WORKERS
                        node.download_file(parts[1], workers=workers)
                    except ValueError:
                        print("Worker count must be an integer.")
            else:
                print("Unknown command. Type help.")
    except KeyboardInterrupt:
        pass
    finally:
        node.close()


if __name__ == "__main__":
    main()
