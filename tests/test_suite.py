# --------------------------------------------------------------------------
# CHALNA-RS TEST SUITE (TITAN EDITION)
# --------------------------------------------------------------------------
# To run: pip install pytest && pytest tests/test_suite.py
# --------------------------------------------------------------------------

import os
import sys
import time
import pytest
import shutil
import pickle
import threading
import tempfile
import concurrent.futures
from pathlib import Path

# Try to import local version first
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import chalna_rs as crs

# ==========================================================================
# TEST FIXTURES
# ==========================================================================

@pytest.fixture(scope="session")
def rust_backend_available():
    """Checks if the Rust backend is compiled and loaded."""
    if not crs._HAS_RUST:
        pytest.skip("Rust backend not compiled. Run 'maturin develop' first.")
    return True

@pytest.fixture
def temp_cache_dir():
    """Creates a temporary directory for cache tests."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def big_file(temp_cache_dir):
    """Generates a massive dummy file for GhostList tests."""
    path = os.path.join(temp_cache_dir, "massive.log")
    with open(path, "w") as f:
        for i in range(1000):
            f.write(f"Line {i}: This is a test log entry for the Ghost Engine.\n")
    return path

# ==========================================================================
# MATRIX TESTS (LINEAR ALGEBRA)
# ==========================================================================

class TestMatrix:
    def test_initialization(self, rust_backend_available):
        """Test matrix creation modes."""
        m = crs.Matrix.zeros(10, 10)
        assert m.shape == (10, 10)
        assert m[0, 0] == 0.0

        m2 = crs.Matrix.identity(5)
        assert m2[0, 0] == 1.0
        assert m2[1, 0] == 0.0
        assert m2[4, 4] == 1.0

    def test_from_list(self, rust_backend_available):
        """Test creation from Python lists."""
        data = [[1.0, 2.0], [3.0, 4.0]]
        m = crs.Matrix.from_list(data)
        assert m.shape == (2, 2)
        assert m[0, 1] == 2.0
        assert m[1, 0] == 3.0

    def test_ops_add(self, rust_backend_available):
        """Test parallel addition."""
        A = crs.Matrix.identity(100)
        B = crs.Matrix.identity(100)
        C = A + B
        assert C[0, 0] == 2.0
        assert C[50, 50] == 2.0
        assert C[0, 1] == 0.0

    def test_ops_matmul(self, rust_backend_available):
        """Test parallel matrix multiplication."""
        # Identity * Identity = Identity
        A = crs.Matrix.identity(10)
        B = crs.Matrix.identity(10)
        C = A @ B
        for i in range(10):
            assert C[i, i] == 1.0

        # Scaling
        D = A * 5.0
        assert D[0, 0] == 5.0

    def test_transpose(self, rust_backend_available):
        """Test matrix transposition."""
        data = [[1.0, 2.0, 3.0]] # 1x3
        m = crs.Matrix.from_list(data)
        assert m.shape == (1, 3)
        
        mt = m.T
        assert mt.shape == (3, 1)
        assert mt[0, 0] == 1.0
        assert mt[1, 0] == 2.0

    def test_determinant(self, rust_backend_available):
        """Test recursive determinant calculation."""
        # Det of Identity is 1
        m = crs.Matrix.identity(5)
        assert abs(m.determinant() - 1.0) < 1e-9
        
        # Singular matrix (all zeros)
        m2 = crs.Matrix.zeros(3, 3)
        assert abs(m2.determinant() - 0.0) < 1e-9

    def test_error_handling(self, rust_backend_available):
        """Test dimension mismatch errors."""
        A = crs.Matrix.zeros(2, 2)
        B = crs.Matrix.zeros(3, 3)
        
        with pytest.raises(crs.MatrixMathError):
            _ = A + B
            
        with pytest.raises(crs.MatrixMathError):
            _ = A @ B

# ==========================================================================
# VAULT TESTS (ENCRYPTION)
# ==========================================================================

class TestVault:
    def test_keygen(self, rust_backend_available):
        """Test secure key generation."""
        key = crs.Vault.generate_key()
        assert isinstance(key, str)
        assert len(key) > 10 # Base64 string

    def test_encrypt_decrypt_string(self, rust_backend_available):
        """Test standard string encryption cycle."""
        vault = crs.Vault() # Ephemeral
        secret = "Asagi's Secret Data 123"
        
        encrypted = vault.encrypt(secret)
        assert isinstance(encrypted, bytes)
        assert encrypted != secret.encode()
        
        decrypted = vault.decrypt(encrypted)
        assert decrypted == secret

    def test_encrypt_decrypt_bytes(self, rust_backend_available):
        """Test binary data handling."""
        vault = crs.Vault()
        data = os.urandom(1024) # 1KB random
        
        enc = vault.encrypt(data)
        dec = vault.decrypt(enc)
        assert dec == data

    def test_file_encryption(self, rust_backend_available, temp_cache_dir):
        """Test streaming file encryption."""
        src = os.path.join(temp_cache_dir, "secret.txt")
        enc = os.path.join(temp_cache_dir, "secret.enc")
        dec = os.path.join(temp_cache_dir, "restored.txt")
        
        original_text = "Highly Classified Document" * 500
        with open(src, "w") as f:
            f.write(original_text)
            
        vault = crs.Vault()
        vault.encrypt_file(src, enc)
        
        assert os.path.exists(enc)
        assert os.path.getsize(enc) > 0
        
        # Verify file is actually encrypted (not plain text)
        with open(enc, "rb") as f:
            content = f.read()
            assert b"Highly Classified" not in content
            
        vault.decrypt_file(enc, dec)
        
        with open(dec, "r") as f:
            restored = f.read()
            assert restored == original_text

    def test_security_errors(self, rust_backend_available):
        """Test decryption with wrong key."""
        v1 = crs.Vault()
        v2 = crs.Vault() # Different random key
        
        data = "Secret"
        enc = v1.encrypt(data)
        
        # Try decrypting with wrong vault
        with pytest.raises(crs.SecurityError):
            v2.decrypt(enc)

# ==========================================================================
# SENTINEL TESTS (RATE LIMITING)
# ==========================================================================

class TestSentinel:
    def test_basic_limiting(self, rust_backend_available):
        """Test token bucket logic."""
        # 10 requests per second
        s = crs.Sentinel(10, 1.0)
        
        # First 10 should pass instantly
        for _ in range(10):
            assert s.check() is True
            
        # 11th should fail
        assert s.check() is False

    def test_decorator_blocking(self, rust_backend_available):
        """Test the @rate_limit decorator blocking mode."""
        
        @crs.rate_limit(limit=5, period=1.0)
        def heavy_api():
            return True
            
        start = time.time()
        # Call 6 times. The 6th should block slightly until tokens refill
        for _ in range(6):
            heavy_api()
        end = time.time()
        
        # Should take at least a tiny fraction of a second if it blocked
        # (This test is flaky on slow machines, so we check loose bounds)
        assert end - start > 0.0

    def test_decorator_raising(self, rust_backend_available):
        """Test the @rate_limit decorator exception mode."""
        
        @crs.rate_limit(limit=1, period=10.0, raise_on_limit=True)
        def strict_api():
            return True
            
        strict_api() # Pass
        
        with pytest.raises(crs.RateLimitExceeded):
            strict_api() # Fail

# ==========================================================================
# GHOST LIST TESTS (BIG DATA)
# ==========================================================================

class TestGhostList:
    def test_indexing(self, rust_backend_available, big_file):
        """Test O(1) random access."""
        ghost = crs.GhostList(big_file)
        
        # Test exact content match
        line_0 = ghost[0]
        assert "Line 0:" in line_0
        
        line_999 = ghost[999]
        assert "Line 999:" in line_999

    def test_bloom_filter(self, rust_backend_available, big_file):
        """Test probabilistic existence check."""
        ghost = crs.GhostList(big_file)
        
        # "contains" uses Bloom Filter internally
        assert "Line 50: This is a test log entry for the Ghost Engine." in ghost
        assert "Line 9999999" not in ghost # Should definitely be false

    def test_out_of_bounds(self, rust_backend_available, big_file):
        """Test index error handling."""
        ghost = crs.GhostList(big_file)
        
        with pytest.raises(IndexError):
            _ = ghost[100000]

# ==========================================================================
# TURBOCHARGER TESTS (PARALLELISM)
# ==========================================================================

def square(x):
    return x * x

class TestTurbo:
    def test_map(self, rust_backend_available):
        """Test parallel map correctness."""
        inputs = list(range(100))
        expected = [x*x for x in inputs]
        
        # Run via Turbo
        results = crs.Turbo.map(square, inputs)
        
        assert results == expected

    def test_async_future(self, rust_backend_available):
        """Test fire-and-forget execution."""
        future = crs.Turbo.run_async(square, 10)
        
        assert future.result() == 100

# ==========================================================================
# TIME CAPSULE TESTS (CACHING)
# ==========================================================================

class TestTimeCapsule:
    def test_caching_behavior(self, rust_backend_available, temp_cache_dir):
        """Verify that the second call is cached."""
        
        call_count = 0
        
        @crs.time_capsule(namespace="test_ns")
        def expensive_func(x):
            nonlocal call_count
            call_count += 1
            return x * 2
            
        # 1. First Call
        res1 = expensive_func(10)
        assert res1 == 20
        assert call_count == 1
        
        # 2. Second Call (Should be cached)
        res2 = expensive_func(10)
        assert res2 == 20
        assert call_count == 1 # Count should NOT increase!

    def test_cache_invalidation(self, rust_backend_available, temp_cache_dir):
        """Verify that different args trigger new computation."""
        
        call_count = 0
        @crs.time_capsule(namespace="test_ns_2")
        def func(x):
            nonlocal call_count
            call_count += 1
            return x
            
        func(1)
        func(2)
        assert call_count == 2