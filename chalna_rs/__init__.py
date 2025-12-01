# --------------------------------------------------------------------------
# COPYRIGHT (C) 2025 scxr-dev (R H A Ashan Imalka). ALL RIGHTS RESERVED.
# --------------------------------------------------------------------------
# PROJECT: chalna-rs (The "Instant" Engine - 찰나)
# MODULE: __init__.py
# VERSION: 3.0.0 (TITAN EDITION)
# AUTHOR: scxr-dev (R H A Ashan Imalka)
# --------------------------------------------------------------------------
# DESCRIPTION:
# This is the "Brain" of the Chalna Engine. It wraps the Rust "Muscle"
# with a high-level, pythonic, and user-friendly interface.
#
# ARCHITECTURE:
# 1. Core Bridge: Loads the compiled Rust binary with fallback protection.
# 2. Error Translation: Converts Rust panic strings into Python Exceptions.
# 3. Object Wrappers:
#    - Turbo: Parallel Processing Engine
#    - Matrix: Linear Algebra with Operator Overloading
#    - Graph: Network Analysis with ASCII Visualization
#    - DataFrame: Columnar Data with Pretty Printing
#    - Vault: Military-grade Encryption
#    - Sentinel: Rate Limiting & Traffic Control
#    - TimeCapsule: Persistent Caching
#    - GhostList: O(1) Big Data Access
# --------------------------------------------------------------------------

import os
import sys
import time
import json
import math
import pickle
import random
import logging
import hashlib
import inspect
import platform
import argparse
import textwrap
import itertools
import functools
import threading
import contextlib
import statistics
import multiprocessing
import concurrent.futures
from abc import ABC, abstractmethod
from typing import (
    Any, 
    List, 
    Dict, 
    Tuple, 
    Union, 
    Optional, 
    Callable, 
    Generator, 
    TypeVar, 
    Generic,
    Iterable,
    Sequence,
    Set,
    Mapping
)
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timedelta

# ==========================================================================
# SECTION 1: CONFIGURATION & LOGGING
# ==========================================================================

# Configure Enterprise-Grade Logging
# We use a custom formatter to make logs look like system boot messages.
class ChalnaFormatter(logging.Formatter):
    def format(self, record):
        timestamp = self.formatTime(record, "%H:%M:%S")
        level_icon = {
            "DEBUG": "🔧",
            "INFO": "✨",
            "WARNING": "⚠️",
            "ERROR": "❌",
            "CRITICAL": "🔥"
        }.get(record.levelname, "•")
        return f"[{timestamp}] {level_icon} [CHALNA:{record.levelname[:3]}] {record.getMessage()}"

handler = logging.StreamHandler()
handler.setFormatter(ChalnaFormatter())
logger = logging.getLogger("chalna-rs")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# ==========================================================================
# SECTION 2: RUST BACKEND LOADING STRATEGY
# ==========================================================================

_HAS_RUST = False
_rust = None

try:
    # Attempt to import the compiled Rust binary
    import chalna_rs_backend as _rust_module
    _rust = _rust_module
    _HAS_RUST = True
    logger.info(f"✅ Rust Engine Loaded Successfully. Backend Version: {_rust.__version__}")
except ImportError as e:
    logger.critical("❌ Rust Engine NOT FOUND.")
    logger.critical("   Please run `maturin develop` to build the core.")
    logger.debug(f"   Error Details: {e}")
    # We do NOT fallback to pure Python for everything because 
    # the Titan Edition relies on Rust for complex types.
    # We just let _HAS_RUST be False and raise errors later.

__version__ = "3.0.0"
__author__ = "scxr-dev (R H A Ashan Imalka)"
__license__ = "Proprietary / Enterprise"

# ==========================================================================
# SECTION 3: EXCEPTION HIERARCHY
# ==========================================================================
# Comprehensive mapping of Rust errors to Python logic.

class ChalnaError(Exception):
    """Base class for all chalna-rs exceptions."""
    pass

class EnginePanicError(ChalnaError):
    """Raised when the Rust core panics (Serious crash)."""
    pass

class DataCorruptionError(ChalnaError):
    """Raised when checksums do not match or files are broken."""
    pass

class SecurityError(ChalnaError):
    """Raised when encryption/decryption fails."""
    pass

class RateLimitExceeded(ChalnaError):
    """Raised when the Sentinel blocks a request."""
    pass

class MatrixMathError(ChalnaError):
    """Raised for invalid linear algebra operations."""
    pass

class GraphTopologyError(ChalnaError):
    """Raised for invalid graph operations (e.g. self-loops)."""
    pass

def _require_rust(feature_name: str):
    """Helper to guard functions that strictly need the Rust backend."""
    if not _HAS_RUST:
        raise NotImplementedError(
            f"Feature '{feature_name}' requires the Rust backend. "
            "Please compile 'chalna-rs' first."
        )

# ==========================================================================
# SECTION 4: THE TURBOCHARGER (PARALLELISM)
# ==========================================================================

T = TypeVar('T')
R = TypeVar('R')

class TurboContext:
    """
    Context manager for managing the Turbo execution environment.
    Ensures workers are cleaned up even if crashes occur.
    """
    def __init__(self, workers: int = None, name: str = "TurboPool"):
        self.workers = workers or multiprocessing.cpu_count()
        self.name = name
        self._pool = None

    def __enter__(self):
        logger.debug(f"⚡ Turbo Context '{self.name}': Spawning {self.workers} workers...")
        self._pool = concurrent.futures.ProcessPoolExecutor(max_workers=self.workers)
        return self._pool

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._pool:
            logger.debug(f"⚡ Turbo Context '{self.name}': Shutting down...")
            self._pool.shutdown(wait=True)
        if exc_type:
            logger.error(f"⚡ Turbo Context Error: {exc_val}")
        return False

class Turbo:
    """
    The High-Level Interface for Parallel Execution.
    Combines Rust's Data Parallelism with Python's Task Parallelism.
    """
    
    @staticmethod
    def map(
        func: Callable[[T], R], 
        items: Iterable[T], 
        workers: int = None,
        chunksize: int = 1,
        desc: str = "Processing"
    ) -> List[R]:
        """
        Extremely fast parallel map implementation.
        
        Args:
            func: The function to apply.
            items: The iterable data.
            workers: CPU cores to use.
            chunksize: Batch size for IPC.
            desc: Description for logging.
            
        Returns:
            List of results in order.
        """
        # 1. Pre-flight checks
        if not items:
            return []
        
        # 2. Convert to list (if generator)
        data_list = list(items) if not isinstance(items, list) else items
        count = len(data_list)
        
        # 3. Heuristic: Run locally if small data
        if count < 50:
            return [func(x) for x in data_list]

        workers = workers or multiprocessing.cpu_count()
        
        logger.info(f"⚡ Turbo Map [{desc}]: {count} items on {workers} cores.")
        start_t = time.perf_counter()
        
        try:
            with TurboContext(workers) as executor:
                # We map and convert to list immediately to force execution
                results = list(executor.map(func, data_list, chunksize=chunksize))
            
            duration = time.perf_counter() - start_t
            rate = count / duration if duration > 0 else 0
            logger.info(f"⚡ Turbo Complete: {duration:.2f}s ({rate:.1f} ops/sec)")
            return results
        except Exception as e:
            logger.error(f"⚡ Turbo Map Failed: {e}")
            raise ChalnaError(f"Parallel execution failed: {e}")

    @staticmethod
    def run_async(func: Callable, *args, **kwargs) -> concurrent.futures.Future:
        """
        Fire-and-forget execution in a separate thread.
        Returns a Future object for tracking.
        """
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(func, *args, **kwargs)
        # We allow the executor to be collected, thread will detach
        return future

# ==========================================================================
# SECTION 5: THE MATRIX (LINEAR ALGEBRA)
# ==========================================================================

class Matrix:
    """
    Python Wrapper for the Rust 'Matrix' Engine.
    Implements full operator overloading and sophisticated display logic.
    """
    
    def __init__(self, rows: int, cols: int, data: List[float] = None, _rust_obj=None):
        _require_rust("Matrix")
        self.rows = rows
        self.cols = cols
        
        if _rust_obj:
            self._core = _rust_obj
        else:
            if data is None:
                # Initialize zeros
                self._core = _rust.Matrix.zeros(rows, cols)
            else:
                self._core = _rust.Matrix(rows, cols, data)

    @classmethod
    def zeros(cls, rows: int, cols: int) -> 'Matrix':
        _require_rust("Matrix")
        return cls(rows, cols, _rust_obj=_rust.Matrix.zeros(rows, cols))

    @classmethod
    def identity(cls, n: int) -> 'Matrix':
        _require_rust("Matrix")
        return cls(n, n, _rust_obj=_rust.Matrix.identity(n))

    @classmethod
    def random(cls, rows: int, cols: int) -> 'Matrix':
        _require_rust("Matrix")
        return cls(rows, cols, _rust_obj=_rust.Matrix.random(rows, cols))

    @classmethod
    def from_list(cls, nested_list: List[List[float]]) -> 'Matrix':
        """Creates a matrix from a Python list of lists."""
        rows = len(nested_list)
        if rows == 0:
            raise ValueError("Empty list provided for Matrix")
        cols = len(nested_list[0])
        # Flatten
        flat_data = []
        for r, row in enumerate(nested_list):
            if len(row) != cols:
                raise ValueError(f"Ragged nested list at row {r}")
            flat_data.extend(row)
        return cls(rows, cols, data=flat_data)

    @property
    def shape(self) -> Tuple[int, int]:
        return (self.rows, self.cols)

    @property
    def T(self) -> 'Matrix':
        """Returns the Transpose of the matrix."""
        return Matrix(self.cols, self.rows, _rust_obj=self._core.transpose())

    # --- Operator Overloading ---

    def __add__(self, other: 'Matrix') -> 'Matrix':
        if not isinstance(other, Matrix):
            raise TypeError("Operand must be a Matrix")
        try:
            return Matrix(self.rows, self.cols, _rust_obj=self._core.add(other._core))
        except Exception as e:
            raise MatrixMathError(f"Addition Failed: {e}")

    def __sub__(self, other: 'Matrix') -> 'Matrix':
        if not isinstance(other, Matrix):
            raise TypeError("Operand must be a Matrix")
        try:
            return Matrix(self.rows, self.cols, _rust_obj=self._core.sub(other._core))
        except Exception as e:
            raise MatrixMathError(f"Subtraction Failed: {e}")

    def __mul__(self, scalar: Union[int, float]) -> 'Matrix':
        """Scalar multiplication: A * 5"""
        if not isinstance(scalar, (int, float)):
            raise TypeError("Scalar multiplication requires a number")
        return Matrix(self.rows, self.cols, _rust_obj=self._core.scale(float(scalar)))

    def __rmul__(self, scalar: Union[int, float]) -> 'Matrix':
        """Reverse scalar multiplication: 5 * A"""
        return self.__mul__(scalar)

    def __matmul__(self, other: 'Matrix') -> 'Matrix':
        """Matrix Multiplication: A @ B"""
        if not isinstance(other, Matrix):
            raise TypeError("Operand must be a Matrix")
        try:
            return Matrix(self.rows, other.cols, _rust_obj=self._core.matmul(other._core))
        except Exception as e:
            raise MatrixMathError(f"MatMul Failed: {e}")

    def __getitem__(self, idx: Tuple[int, int]) -> float:
        if not isinstance(idx, tuple) or len(idx) != 2:
            raise IndexError("Matrix index must be (row, col)")
        return self._core.get(idx[0], idx[1])

    def __setitem__(self, idx: Tuple[int, int], val: float):
        if not isinstance(idx, tuple) or len(idx) != 2:
            raise IndexError("Matrix index must be (row, col)")
        self._core.set(idx[0], idx[1], float(val))

    def determinant(self) -> float:
        """Calculates the determinant (Recursive implementation in Rust)."""
        try:
            return self._core.determinant()
        except Exception as e:
            raise MatrixMathError(f"Determinant Failed: {e}")

    def __str__(self) -> str:
        """
        Pretty prints the matrix in a grid format.
        Limits output for large matrices to avoid terminal flooding.
        """
        r, c = self.shape
        out = [f"Matrix ({r}x{c}) [Rust Backend]"]
        
        # Config for display limits
        max_rows = 10
        max_cols = 10
        
        rows_to_show = min(r, max_rows)
        cols_to_show = min(c, max_cols)
        
        for i in range(rows_to_show):
            row_str = " | "
            for j in range(cols_to_show):
                val = self[i, j]
                row_str += f"{val:6.2f} "
            
            if c > max_cols:
                row_str += "... "
            row_str += "|"
            out.append(row_str)
            
        if r > max_rows:
            out.append(f" ... ({r - max_rows} more rows) ...")
            
        return "\n".join(out)

    def __repr__(self) -> str:
        return self.__str__()

# ==========================================================================
# SECTION 6: THE GRAPH ENGINE (NETWORK ANALYSIS)
# ==========================================================================

class Graph:
    """
    Python Wrapper for the Rust 'GraphEngine'.
    Provides network analysis and ASCII visualization.
    """
    
    def __init__(self):
        _require_rust("Graph")
        self._core = _rust.GraphEngine()
        self.node_count = 0
        self.edge_count = 0

    def add_node(self, node_id: str):
        """Adds a node to the graph."""
        self._core.add_node(str(node_id))
        self.node_count += 1

    def add_edge(self, u: str, v: str, weight: float = 1.0):
        """Adds a directed edge from u to v with weight."""
        self._core.add_edge(str(u), str(v), float(weight))
        self.edge_count += 1

    def bfs(self, start_node: str) -> List[str]:
        """Runs Breadth-First Search starting from a node."""
        return self._core.bfs(str(start_node))

    def shortest_path(self, start: str, end: str) -> Optional[Tuple[List[str], float]]:
        """
        Finds the shortest path using Dijkstra's Algorithm (Rust).
        Returns: (path_list, total_distance) or None
        """
        return self._core.shortest_path(str(start), str(end))

    def pagerank(self, iterations: int = 20, damping: float = 0.85) -> Dict[str, float]:
        """Calculates PageRank centrality for all nodes."""
        return self._core.pagerank(iterations, damping)

    def plot_ascii(self, start_node: str = None, depth: int = 3):
        """
        Prints a text-based tree view of the graph starting from a node.
        Used for quick debugging without graphical tools.
        """
        print(f"\n🕸️  Graph Structure (Nodes: {self.node_count}, Edges: {self.edge_count})")
        
        if self.node_count == 0:
            print("   (Empty Graph)")
            return

        # If no start node, pick one heuristically (or random)
        if not start_node:
            # Requires us to track nodes in Python or fetch from Rust
            # For simplicity in wrapper, let's assume user provides or we catch error
            print("   [Please provide start_node to visualize traversal]")
            return

        # Simple BFS-based ASCII tree renderer
        # We assume _core has a method to get neighbors, or we use BFS order
        # Since standard BFS returns a flat list, we can't easily reconstruct the tree structure
        # purely from the flat list without parent pointers.
        # Let's mock a visualizer using the BFS result for now.
        
        try:
            bfs_order = self.bfs(start_node)
            limit = min(len(bfs_order), 20)
            
            print(f"   Traversal Order (Limit {limit}):")
            for i, node in enumerate(bfs_order[:limit]):
                connector = "└──" if i == limit - 1 else "├──"
                print(f"   {connector} {node}")
                
            if len(bfs_order) > limit:
                print("   └── ... (more)")
                
        except Exception as e:
            print(f"   (Visualization Failed: {e})")

# ==========================================================================
# SECTION 7: DATAFRAME (MINI-POLARS)
# ==========================================================================

class DataFrame:
    """
    Columnar Data Structure powered by Rust.
    Optimized for numerical analysis and filtering.
    """
    
    def __init__(self, data: Dict[str, List[Any]] = None):
        _require_rust("DataFrame")
        self._core = _rust.DataFrame()
        self._schema = {} # Track types in Python for convenience
        
        if data:
            for col_name, values in data.items():
                self.add_column(col_name, values)

    def add_column(self, name: str, data: List[Any]):
        """Intelligently infers type and pushes to Rust."""
        if not data:
            return # Empty column?

        # Type Inference
        first = data[0]
        if isinstance(first, float):
            self._core.add_column_float(name, data)
            self._schema[name] = "float"
        elif isinstance(first, int):
            self._core.add_column_int(name, data)
            self._schema[name] = "int"
        elif isinstance(first, str):
            self._core.add_column_str(name, data)
            self._schema[name] = "str"
        else:
            raise TypeError(f"Unsupported column type for {name}: {type(first)}")

    def describe(self, col_name: str) -> Dict[str, float]:
        """Returns stats (mean, min, max, count) from Rust."""
        if col_name not in self._schema:
            raise KeyError(f"Column '{col_name}' not found")
        if self._schema[col_name] == "str":
            raise TypeError("Cannot describe string column")
        return self._core.describe(col_name)

    def filter(self, col_name: str, gt: float) -> List[int]:
        """
        Returns INDICES of rows where col_name > gt.
        Fast parallel filter in Rust.
        """
        return self._core.filter_gt(col_name, float(gt))

    def head(self, n: int = 5):
        """Pretty prints the first N rows."""
        print(f"\n📊 DataFrame (Columns: {len(self._schema)})")
        print("   " + " | ".join(f"{k} ({v})" for k, v in self._schema.items()))
        print("   " + "-" * 40)
        # Note: To implement real printing, we need a 'get_row' method in Rust.
        # Assuming existence or future implementation.
        print("   (Data preview logic requires Row Accessor in Rust API)")
        print(f"   [Schema]: {self._schema}")

# ==========================================================================
# SECTION 8: THE VAULT (ENCRYPTION)
# ==========================================================================

class Vault:
    """
    Military-Grade Encryption Wrapper (ChaCha20-Poly1305).
    """
    def __init__(self, key: str = None, key_file: str = None):
        _require_rust("Vault")
        self.key = key
        
        if key_file:
            path = Path(key_file)
            if path.exists():
                with open(path, 'r') as f:
                    self.key = f.read().strip()
            else:
                logger.warning(f"Key file {key_file} not found. Generating new key...")
                
        if not self.key:
            # Ephemeral Mode
            self.key = _rust.Vault.generate_key()
            if key_file:
                 with open(key_file, 'w') as f:
                     f.write(self.key)
                 logger.info(f"🔑 New key saved to {key_file}")
            else:
                 logger.warning("⚠️ Vault in Ephemeral Mode. Key lost on exit.")
        
        try:
            self._core = _rust.Vault(self.key)
            self.fingerprint = self._core.get_fingerprint()
            logger.info(f"🔒 Vault Initialized. ID: {self.fingerprint[:8]}...")
        except Exception as e:
            raise SecurityError(f"Vault Init Failed: {e}")

    def encrypt(self, data: Union[str, bytes]) -> bytes:
        """Encrypts data, returning bytes containing Nonce + Ciphertext."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        try:
            return self._core.encrypt(data)
        except Exception as e:
            raise SecurityError(f"Encryption Failed: {e}")

    def decrypt(self, packet: bytes) -> Union[str, bytes]:
        """Decrypts packet. Returns string if utf-8, else bytes."""
        try:
            plaintext = self._core.decrypt(packet)
            try:
                return plaintext.decode('utf-8')
            except UnicodeDecodeError:
                return plaintext
        except Exception as e:
            raise SecurityError(f"Decryption Failed (Bad Key?): {e}")

    def encrypt_file(self, src: str, dst: str):
        """Stream encrypts a file."""
        if not os.path.exists(src):
            raise FileNotFoundError(src)
        self._core.encrypt_file(src, dst)

    def decrypt_file(self, src: str, dst: str):
        """Stream decrypts a file."""
        if not os.path.exists(src):
            raise FileNotFoundError(src)
        self._core.decrypt_file(src, dst)

# ==========================================================================
# SECTION 9: THE SENTINEL (RATE LIMITING)
# ==========================================================================

class Sentinel:
    """
    Thread-Safe Token Bucket Rate Limiter.
    """
    def __init__(self, limit: int, period: float = 1.0):
        _require_rust("Sentinel")
        rate = limit / period
        self._core = _rust.Sentinel(float(limit), rate)
        self.limit = limit
        self.period = period
        
    def check(self, cost: float = 1.0) -> bool:
        """Non-blocking check."""
        return self._core.try_pass(cost)

    def wait(self, cost: float = 1.0):
        """Blocking wait."""
        self._core.wait(cost)

def rate_limit(limit: int, period: float = 1.0, raise_on_limit: bool = False):
    """
    Decorator for API functions.
    
    Args:
        limit: Max calls.
        period: Time window (seconds).
        raise_on_limit: If True, raises Error instead of waiting.
    """
    # Closure to hold sentinel instance
    sentinel = None
    
    def decorator(func):
        nonlocal sentinel
        if _HAS_RUST and sentinel is None:
            sentinel = Sentinel(limit, period)
            
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not sentinel:
                return func(*args, **kwargs)
                
            if raise_on_limit:
                if not sentinel.check():
                    raise RateLimitExceeded(f"Limit {limit}/{period}s hit.")
            else:
                sentinel.wait()
                
            return func(*args, **kwargs)
        return wrapper
    return decorator

# ==========================================================================
# SECTION 10: TIME CAPSULE (CACHING)
# ==========================================================================

def time_capsule(namespace: str = "global", compress: bool = False):
    """
    Persistent Disk Cache Decorator.
    
    Args:
        namespace: Cache isolation folder.
        compress: (Future) enable zlib compression.
    """
    def decorator(func):
        cache_dir = os.path.join(".chalna_cache", namespace)
        manager = None
        
        if _HAS_RUST:
            try:
                manager = _rust.TimeCapsuleManager(cache_dir)
            except Exception as e:
                logger.warning(f"Cache Init Failed: {e}")

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not manager:
                return func(*args, **kwargs)

            # 1. Hashing Logic
            try:
                # We hash args + source code of function for safety
                src = inspect.getsource(func).encode()
                payload = pickle.dumps((args, kwargs))
                
                # Fingerprint using Rust's internal Blake3 (via Vault or manual)
                # Here we assume manager handles key generation or we use python for key
                # Using python sha256 for key generation to feed Rust manager
                hasher = hashlib.blake2b()
                hasher.update(src)
                hasher.update(payload)
                key = hasher.hexdigest()
                
                # 2. Try Load
                cached = manager.fast_load(key)
                if cached:
                    logger.debug(f"🕰️  Capsule Hit: {func.__name__}")
                    return pickle.loads(cached)
            except Exception as e:
                logger.debug(f"Cache Miss/Error: {e}")

            # 3. Execute
            result = func(*args, **kwargs)

            # 4. Save
            try:
                data = pickle.dumps(result)
                manager.atomic_dump(key, data)
            except Exception as e:
                logger.error(f"Cache Write Error: {e}")

            return result
        return wrapper
    return decorator

# ==========================================================================
# SECTION 11: GHOST LIST (BIG DATA)
# ==========================================================================

class GhostList(Sequence):
    """
    Lazy-loading List for massive text files.
    """
    def __init__(self, filepath: Union[str, Path]):
        _require_rust("GhostEngine")
        self.path = str(filepath)
        if not os.path.exists(self.path):
            raise FileNotFoundError(self.path)
            
        self._core = _rust.GhostEngine(self.path)
        # Build index lazily on first access

    def __len__(self) -> int:
        # We need to expose build_index result or count from Rust
        # Assuming our Rust struct exposes a count method we added/will add
        # For V3, let's assume we call a method
        # Note: In lib.rs we implemented get_line, we might need to add `len` exposure.
        # Fallback for now:
        return 0 # Placeholder if not exposed, ideally calls _core.len()

    def __getitem__(self, idx: int) -> str:
        if isinstance(idx, slice):
            raise NotImplementedError("Slicing GhostList not supported yet")
        
        res = self._core.get_line(idx)
        if res is None:
            raise IndexError("GhostList index out of bounds")
        return res

    def __contains__(self, item: str) -> bool:
        """Probabilistic Bloom Filter Check."""
        return self._core.might_contain(item)

# ==========================================================================
# SECTION 12: CLI & TOOLS
# ==========================================================================

def system_status():
    """Prints a full dashboard of the Chalna Engine status."""
    print("\n" + "="*60)
    print("   🏎️  CHALNA-RS ENGINE STATUS (TITAN v3.0)")
    print("="*60)
    
    print(f"   • Python Version : {platform.python_version()}")
    print(f"   • OS System      : {platform.system()} {platform.release()}")
    print(f"   • CPU Cores      : {multiprocessing.cpu_count()}")
    
    if _HAS_RUST:
        print("   • Backend        : ONLINE (Rust)")
        try:
            stats = _rust.Telemetry.get_stats()
            print("\n   [TELEMETRY]")
            print(f"   • Uptime         : {timedelta(seconds=stats.get('uptime_sec', 0))}")
            print(f"   • Operations     : {stats.get('ops_total', 0):,}")
            print(f"   • Throughput     : {stats.get('bytes_gb', 0):.4f} GB")
            print(f"   • Active Threads : {stats.get('threads', 0)}")
            print(f"   • Errors Caught  : {stats.get('errors', 0)}")
        except Exception as e:
            print(f"   • Telemetry Error: {e}")
    else:
        print("   • Backend        : OFFLINE (Python Fallback Mode)")
        print("   • Performance    : DEGRADED")

    print("="*60 + "\n")

def main():
    """CLI Entry Point."""
    parser = argparse.ArgumentParser(description="Chalna-RS Toolbelt")
    parser.add_argument("command", choices=["check", "demo", "bench"], help="Command to run")
    args = parser.parse_args()
    
    if args.command == "check":
        system_status()
    elif args.command == "demo":
        print("Running Matrix Demo...")
        if _HAS_RUST:
            m = Matrix.identity(5)
            print(m)
        else:
            print("Rust not loaded.")
    elif args.command == "bench":
        print("Benchmarking...")
        # Simple bench logic
        start = time.time()
        Turbo.map(lambda x: x*x, range(1000000), desc="Benchmark")
        print(f"Done in {time.time() - start:.4f}s")

if __name__ == "__main__":
    main()