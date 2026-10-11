import os
import hashlib
import json

CHUNK_SIZE = 1024  # 1024 bytes per chunk

def generate_manifest(file_path):
    """
    Splits a file into 1024-byte chunks, computes SHA-256 hashes for each,
    and returns a manifest dictionary containing file metadata and chunk hashes.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    filename = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    chunks_meta = []

    with open(file_path, "rb") as f:
        chunk_index = 0
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            
            # Compute SHA-256 hash of the chunk
            chunk_hash = hashlib.sha256(data).hexdigest()
            chunks_meta.append({
                "index": chunk_index,
                "size": len(data),
                "hash": chunk_hash
            })
            chunk_index += 1

    manifest = {
        "filename": filename,
        "total_size": file_size,
        "total_chunks": len(chunks_meta),
        "chunks": chunks_meta
    }
    
    return manifest

def get_specific_chunk(file_path, index):
    """
    Reads and returns the raw binary data for a specific chunk index from a file.
    """
    with open(file_path, "rb") as f:
        f.seek(index * CHUNK_SIZE)
        data = f.read(CHUNK_SIZE)
        return data

def verify_chunk_data(data, expected_hash):
    """
    Cryptographically verifies if the received chunk matches the expected SHA-256 hash.
    """
    computed_hash = hashlib.sha256(data).hexdigest()
    return computed_hash == expected_hash

def reassemble_file(output_path, received_chunks, total_chunks):
    """
    Takes a dictionary of received chunks {index: data}, verifies all indices 
    are present, and writes them sequentially to reassemble the final file.
    """
    with open(output_path, "wb") as f:
        for i in range(total_chunks):
            if i not in received_chunks:
                raise ValueError(f"Missing chunk {i}, cannot reassemble file.")
            f.write(received_chunks[i])
    print(f"[+] File successfully reassembled at: {output_path}")
