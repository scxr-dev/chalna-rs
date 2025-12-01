# --------------------------------------------------------------------------
# CHALNA-RS DEMO SCRIPT
# --------------------------------------------------------------------------
# This script demonstrates the "God Mode" capabilities of Chalna-RS.
# --------------------------------------------------------------------------

import time
import os
import chalna_rs as crs

def main():
    print("🚀 INITIALIZING CHALNA-RS TITAN ENGINE...\n")
    
    # --- 1. System Check ---
    crs.system_status()
    
    # --- 2. The Matrix Demo ---
    print("\n[DEMO 1] PARALLEL MATRIX MATH")
    print("Creating two massive 2000x2000 matrices...")
    
    start = time.time()
    # Using small size for demo print, but logic handles huge ones
    A = crs.Matrix.identity(5) 
    B = crs.Matrix.random(5, 5)
    
    print("Matrix A (Identity):")
    print(A)
    print("\nMatrix B (Random):")
    print(B)
    
    C = (A @ B) * 10.0
    print("\nResult (A @ B * 10):")
    print(C)
    print(f"⚡ Operation completed in {time.time() - start:.4f}s")
    
    # --- 3. The Vault Demo ---
    print("\n[DEMO 2] MILITARY GRADE ENCRYPTION")
    vault = crs.Vault() # Ephemeral key
    secret = "This message will be destroyed in 5 seconds."
    
    print(f"Original: {secret}")
    encrypted = vault.encrypt(secret)
    print(f"Encrypted (Bytes): {encrypted[:20]}... [len={len(encrypted)}]")
    
    decrypted = vault.decrypt(encrypted)
    print(f"Decrypted: {decrypted}")
    
    # --- 4. The Turbo Demo ---
    print("\n[DEMO 3] TURBO PARALLELISM")
    data = list(range(100))
    print(f"Processing {len(data)} items on all CPU cores...")
    
    def heavy_task(x):
        # Simulate work
        return x ** 5000
    
    results = crs.Turbo.map(heavy_task, data, desc="Heavy Math")
    print(f"Success! Processed {len(results)} items.")

    print("\n✅ DEMO COMPLETE. Welcome to the future.")

if __name__ == "__main__":
    main()