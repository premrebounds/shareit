import os
import sys
import time
from node import P2PNode

def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <port> [bootstrap_port1 bootstrap_port2 ...]")
        print("Example (standalone): python main.py 6001")
        print("Example (with initial peers): python main.py 6002 6001 6003")
        sys.exit(1)

    port = int(sys.argv[1])
    shared_dir = f"./peer_node_{port}"
    
    # Parse any number of optional bootstrap peers passed via command line
    bootstrap_ports = []
    if len(sys.argv) > 2:
        bootstrap_ports = [int(p) for p in sys.argv[2:]]

    # Initialize node on localhost with a dynamic port and unique folder
    node = P2PNode("127.0.0.1", port, shared_dir)
    time.sleep(1)  # Give the server thread a moment to bind

    # Connect to initial bootstrap peers if provided
    for bp in bootstrap_ports:
        node.add_peer("127.0.0.1", bp)

    print(f"\n========================================")
    print(f"[*] Node started successfully on Port {port}")
    print(f"[*] Shared Directory: {shared_dir}")
    print(f"========================================")

    while True:
        print(f"\n--- Node Menu (Port {port}) ---")
        print("1. List files in shared folder")
        print("2. Search for a file on the network")
        print("3. Download a file concurrently from peers")
        print("4. Dynamically connect to a new peer port")
        print("5. View active connected peers")
        print("6. Exit / Quit")
        
        choice = input("Enter choice (1-6): ").strip()

        if choice == "1":
            files = os.listdir(shared_dir) if os.path.exists(shared_dir) else []
            print(f"Files in {shared_dir}: {files}")

        elif choice == "2":
            fname = input("Enter filename to search: ").strip()
            manifest, source = node.search_file_network(fname)
            if manifest:
                print(f"[+] Found! Manifest: {manifest}")
                print(f"[+] Primary Seeder: {source}")
            else:
                print(f"[-] File '{fname}' not found on any peer.")

        elif choice == "3":
            fname = input("Enter filename to download: ").strip()
            node.download_file(fname)

        elif choice == "4":
            try:
                new_port = int(input("Enter target peer port to connect: ").strip())
                node.add_peer("127.0.0.1", new_port)
            except ValueError:
                print("[-] Invalid port number.")

        elif choice == "5":
            print(f"[{port}] Active Connected Peers: {node.peers}")

        elif choice == "6":
            print(f"Shutting down node on port {port}...")
            node.running = False
            break
        else:
            print("Invalid choice. Please pick between 1 and 6.")

if __name__ == "__main__":
    main()
