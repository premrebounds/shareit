import os
import shutil
from utils import generate_manifest, get_specific_chunk, verify_chunk_data, reassemble_file

def run_tests():
    print("[*] Running P2P System Core Tests...")
    
    # 1. Setup a dummy test file
    test_dir = "./test_sandbox"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)
    
    original_file = os.path.join(test_dir, "sample.txt")
    reconstructed_file = os.path.join(test_dir, "reconstructed.txt")
    
    sample_content = b"Hello, P2P network! This is a test file to verify chunking, SHA-256 hashing, and sequential reassembly logic." * 10
    
    with open(original_file, "wb") as f:
        f.write(sample_content)
    print("[+] Created dummy test file.")

    # 2. Test Manifest Generation & Chunking
    manifest = generate_manifest(original_file)
    assert manifest["filename"] == "sample.txt", "Filename mismatch in manifest!"
    assert manifest["total_size"] == len(sample_content), "Size mismatch in manifest!"
    assert manifest["total_chunks"] > 0, "Total chunks should be greater than zero!"
    print(f"[+] Manifest generated successfully: {manifest['total_chunks']} chunks found.")

    # 3. Test Individual Chunk Retrieval & Hash Verification
    received_chunks = {}
    for chunk_meta in manifest["chunks"]:
        idx = chunk_meta["index"]
        expected_hash = chunk_meta["hash"]
        
        # Pull chunk from file
        chunk_data = get_specific_chunk(original_file, idx)
        
        # Verify hash integrity
        is_valid = verify_chunk_data(chunk_data, expected_hash)
        assert is_valid, f"Hash verification failed for chunk {idx}!"
        
        received_chunks[idx] = chunk_data
    print("[+] All chunks successfully retrieved and verified via SHA-256 hashes.")

    # 4. Test File Reassembly
    reassemble_file(reconstructed_file, received_chunks, manifest["total_chunks"])
    
    # 5. Compare Original vs Reconstructed
    with open(reconstructed_file, "rb") as f:
        reconstructed_content = f.read()
        
    assert reconstructed_content == sample_content, "Reconstructed file content does not match original!"
    print("[+] Reconstructed file integrity check passed perfectly!")

    # Cleanup sandbox
    shutil.rmtree(test_dir)
    print("[*] All tests passed successfully!")

if __name__ == "__main__":
    run_tests()
