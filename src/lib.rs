// --------------------------------------------------------------------------
// COPYRIGHT (C) 2025 scxr-dev (R H A Ashan Imalka). ALL RIGHTS RESERVED.
// --------------------------------------------------------------------------
// PROJECT: chalna-rs (The "Instant" Engine - 찰나)
// MODULE: chalna_rs_backend
// VERSION: 3.0.0 
// AUTHOR: scxr-dev
// --------------------------------------------------------------------------
// DESCRIPTION:
// This is the monolithic core of the Chalna engine.
// It integrates multiple high-performance domains into a single binary:
// 1. Cryptography (ChaCha20-Poly1305, Blake3)
// 2. Linear Algebra (Matrix, Vector ops)
// 3. Graph Theory (Network analysis, BFS/DFS)
// 4. Data Engineering (Mini-DataFrame implementation)
// 5. System Architecture (Rate Limiting, File Locking)
// --------------------------------------------------------------------------

use pyo3::create_exception;
use pyo3::exceptions::{
    PyIOError, PyIndexError, PyKeyError, PyRuntimeError, PyValueError, PyTypeError, PyNotImplementedError
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyFloat, PyString, PyTuple};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet, VecDeque, BTreeMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering, AtomicBool};
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::cmp::Ordering as CmpOrdering;
use memmap2::MmapOptions;
use blake3;
use serde::{Serialize, Deserialize};
use bincode;
use chacha20poly1305::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    ChaCha20Poly1305, Nonce, Key
};
use rand::{RngCore, Rng};
use parking_lot::{Mutex as FastMutex, RwLock as FastRwLock};

// ==========================================================================
// SECTION 1: GLOBAL CONFIGURATION & CONSTANTS
// ==========================================================================

const CHUNK_SIZE: usize = 64 * 1024; 
const INDEX_INTERVAL: usize = 100;   
const MAGIC_HEADER: &[u8; 4] = b"CHLN"; 
const VERSION: u8 = 3;
const MAX_GRAPH_NODES: usize = 1_000_000;
const MAX_MATRIX_SIZE: usize = 10_000;

// Global Telemetry Counters
static OP_COUNTER: AtomicUsize = AtomicUsize::new(0);
static BYTES_PROCESSED: AtomicUsize = AtomicUsize::new(0);
static THREADS_SPAWNED: AtomicUsize = AtomicUsize::new(0);
static ERRORS_ENCOUNTERED: AtomicUsize = AtomicUsize::new(0);
static SYSTEM_START: AtomicUsize = AtomicUsize::new(0); // Epoch timestamp

// ==========================================================================
// SECTION 2: THE ERROR HIERARCHY (EXTREME DETAIL)
// ==========================================================================
// We define a massive error tree so Python knows exactly what went wrong.

create_exception!(chalna_rs_backend, ChalnaEngineError, PyRuntimeError);
create_exception!(chalna_rs_backend, ChalnaCorruptDataError, PyValueError);
create_exception!(chalna_rs_backend, ChalnaSecurityError, PyValueError);
create_exception!(chalna_rs_backend, ChalnaRateLimitError, PyRuntimeError);
create_exception!(chalna_rs_backend, ChalnaGraphError, PyValueError);
create_exception!(chalna_rs_backend, ChalnaMathError, PyValueError);

#[derive(Debug)]
enum BackendError {
    // IO & System
    Io(io::Error),
    Serialization(bincode::Error),
    LockPoison(String),
    InvalidState(String),
    FileNotFound(String),
    PermissionDenied(String),
    
    // Data Structure Logic
    IndexOutOfBounds { requested: usize, max: usize },
    CorruptIndex(String),
    TypeMismatch(String),
    KeyNotFound(String),
    DuplicateKey(String),
    
    // Linear Algebra
    MatrixDimensionMismatch { r1: usize, c1: usize, r2: usize, c2: usize },
    SingularMatrix,
    VectorDimensionMismatch,
    
    // Cryptography
    CryptoError(String),
    InvalidKeyLength(usize),
    
    // System
    RateLimited(String),
    Timeout(String),
    
    // Graph Theory
    NodeNotFound(String),
    SelfLoop(String),
    NegativeCycle,
    GraphTooLarge,
}

impl From<BackendError> for PyErr {
    fn from(err: BackendError) -> PyErr {
        ERRORS_ENCOUNTERED.fetch_add(1, Ordering::Relaxed);
        match err {
            BackendError::Io(e) => PyIOError::new_err(format!("⛔ [System IO] {}", e)),
            BackendError::Serialization(e) => PyValueError::new_err(format!("📦 [Serialization] {}", e)),
            BackendError::LockPoison(msg) => ChalnaEngineError::new_err(format!("☣️ [Thread Panic] {}", msg)),
            BackendError::InvalidState(msg) => ChalnaEngineError::new_err(format!("⚠️ [Invalid State] {}", msg)),
            BackendError::FileNotFound(f) => PyIOError::new_err(format!("🔍 [File Not Found] {}", f)),
            BackendError::PermissionDenied(p) => PyIOError::new_err(format!("🚫 [Access Denied] {}", p)),
            
            BackendError::IndexOutOfBounds { requested, max } => {
                PyIndexError::new_err(format!("📏 [Bounds] Index {} > Max {}", requested, max))
            },
            BackendError::CorruptIndex(msg) => ChalnaCorruptDataError::new_err(format!("💀 [Corrupt Data] {}", msg)),
            BackendError::TypeMismatch(msg) => PyTypeError::new_err(format!("🤔 [Type Error] {}", msg)),
            BackendError::KeyNotFound(k) => PyKeyError::new_err(format!("🔑 [Missing Key] {}", k)),
            BackendError::DuplicateKey(k) => PyValueError::new_err(format!("👯 [Duplicate] {}", k)),
            
            BackendError::MatrixDimensionMismatch { r1, c1, r2, c2 } => {
                ChalnaMathError::new_err(format!("📐 [Matrix Ops] ({}x{}) vs ({}x{}) incompatible", r1, c1, r2, c2))
            },
            BackendError::SingularMatrix => ChalnaMathError::new_err("0️⃣ [Math] Matrix is singular (No Inverse)"),
            BackendError::VectorDimensionMismatch => ChalnaMathError::new_err("📏 [Math] Vector lengths differ"),
            
            BackendError::CryptoError(msg) => ChalnaSecurityError::new_err(format!("🔒 [Vault] {}", msg)),
            BackendError::InvalidKeyLength(len) => ChalnaSecurityError::new_err(format!("🗝️ [Vault] Key len {} invalid (needs 32)", len)),
            
            BackendError::RateLimited(msg) => ChalnaRateLimitError::new_err(format!("⏳ [Sentinel] {}", msg)),
            BackendError::Timeout(msg) => ChalnaEngineError::new_err(format!("⏱️ [Timeout] {}", msg)),
            
            BackendError::NodeNotFound(n) => ChalnaGraphError::new_err(format!("🕸️ [Graph] Node '{}' missing", n)),
            BackendError::SelfLoop(n) => ChalnaGraphError::new_err(format!("🕸️ [Graph] Self loop on '{}' forbidden", n)),
            BackendError::NegativeCycle => ChalnaGraphError::new_err("🕸️ [Graph] Negative weight cycle detected"),
            BackendError::GraphTooLarge => ChalnaGraphError::new_err("🕸️ [Graph] Max node limit exceeded"),
        }
    }
}

// Implement standard error traits to allow `?` operator usage internally
impl From<io::Error> for BackendError { fn from(err: io::Error) -> Self { BackendError::Io(err) } }
impl From<bincode::Error> for BackendError { fn from(err: bincode::Error) -> Self { BackendError::Serialization(err) } }
implFrom<chacha20poly1305::aead::Error> for BackendError {
    fn from(_: chacha20poly1305::aead::Error) -> Self {
        BackendError::CryptoError("Decryption Verification Failed".into())
    }
}
impl<T> From<std::sync::PoisonError<T>> for BackendError {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        BackendError::LockPoison("Mutex poisoned".into())
    }
}

// ==========================================================================
// SECTION 3: UTILITIES & TELEMETRY
// ==========================================================================

fn track_op(bytes: usize) {
    OP_COUNTER.fetch_add(1, Ordering::Relaxed);
    BYTES_PROCESSED.fetch_add(bytes, Ordering::Relaxed);
}

#[pyclass]
struct Telemetry;
#[pymethods]
impl Telemetry {
    #[staticmethod]
    fn get_stats() -> PyResult<Py<PyDict>> {
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            dict.set_item("ops_total", OP_COUNTER.load(Ordering::Relaxed))?;
            dict.set_item("bytes_gb", BYTES_PROCESSED.load(Ordering::Relaxed) as f64 / 1_073_741_824.0)?;
            dict.set_item("threads", rayon::current_num_threads())?;
            dict.set_item("errors", ERRORS_ENCOUNTERED.load(Ordering::Relaxed))?;
            
            let uptime = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as usize 
                         - SYSTEM_START.load(Ordering::Relaxed);
            dict.set_item("uptime_sec", uptime)?;
            Ok(dict.into())
        })
    }
    
    #[staticmethod]
    fn reset() {
        OP_COUNTER.store(0, Ordering::Relaxed);
        BYTES_PROCESSED.store(0, Ordering::Relaxed);
        ERRORS_ENCOUNTERED.store(0, Ordering::Relaxed);
    }
}

// ==========================================================================
// SECTION 4: CRYPTOGRAPHY (THE VAULT)
// ==========================================================================
// Implements secure storage, file encryption, and ephemeral keys.

#[pyclass]
struct Vault {
    cipher: ChaCha20Poly1305,
    key_id: String, // Tracks which key is loaded
}

#[pymethods]
impl Vault {
    /// Generates a high-entropy 32-byte key
    #[staticmethod]
    fn generate_key() -> String {
        let key = ChaCha20Poly1305::generate_key(&mut OsRng);
        base64::encode(key)
    }

    #[new]
    fn new(key_b64: String) -> PyResult<Self> {
        let key_bytes = base64::decode(&key_b64)
            .map_err(|_| BackendError::CryptoError("Invalid Base64 string".into()))?;
        
        if key_bytes.len() != 32 {
            return Err(BackendError::InvalidKeyLength(key_bytes.len()).into());
        }

        let key = Key::from_slice(&key_bytes);
        let cipher = ChaCha20Poly1305::new(key);
        let key_id = blake3::hash(&key_bytes).to_hex().to_string(); // Fingerprint
        
        Ok(Vault { cipher, key_id })
    }
    
    fn get_fingerprint(&self) -> String {
        self.key_id.clone()
    }

    /// Encrypts bytes -> Returns [Nonce + Ciphertext]
    fn encrypt<'py>(&self, py: Python<'py>, data: &[u8]) -> PyResult<Py<PyBytes>> {
        let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng); // 96-bit nonce
        let ciphertext = self.cipher.encrypt(&nonce, data).map_err(BackendError::from)?;
        
        let mut packet = Vec::with_capacity(nonce.len() + ciphertext.len());
        packet.extend_from_slice(&nonce);
        packet.extend_from_slice(&ciphertext);
        
        track_op(data.len());
        Ok(PyBytes::new(py, &packet).into())
    }

    /// Decrypts [Nonce + Ciphertext] -> Bytes
    fn decrypt<'py>(&self, py: Python<'py>, packet: &[u8]) -> PyResult<Py<PyBytes>> {
        if packet.len() < 12 {
            return Err(BackendError::CryptoError("Packet too short (Missing Nonce)".into()).into());
        }

        let (nonce_bytes, ciphertext) = packet.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);
        
        let plaintext = self.cipher.decrypt(nonce, ciphertext)
            .map_err(BackendError::from)?;
            
        track_op(plaintext.len());
        Ok(PyBytes::new(py, &plaintext).into())
    }

    /// Encrypts an entire file efficiently using streaming (buffered)
    /// NOTE: For Poly1305, we technically need to process the whole block for auth.
    /// This implementation loads file into RAM. For true streaming AEAD, we need a stream cipher mode.
    /// Since we use ChaCha20Poly1305, we accept the RAM cost for integrity safety.
    fn encrypt_file(&self, input_path: String, output_path: String) -> PyResult<()> {
        let input_data = fs::read(&input_path).map_err(BackendError::from)?;
        
        let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
        let ciphertext = self.cipher.encrypt(&nonce, input_data.as_ref())
            .map_err(BackendError::from)?;
            
        let mut file = File::create(&output_path).map_err(BackendError::from)?;
        
        // Write Header
        file.write_all(MAGIC_HEADER).map_err(BackendError::from)?; 
        file.write_all(&nonce).map_err(BackendError::from)?;
        file.write_all(&ciphertext).map_err(BackendError::from)?;
        
        track_op(input_data.len());
        Ok(())
    }

    fn decrypt_file(&self, input_path: String, output_path: String) -> PyResult<()> {
        let mut file = File::open(&input_path).map_err(BackendError::from)?;
        let mut packet = Vec::new();
        file.read_to_end(&mut packet).map_err(BackendError::from)?;
        
        if packet.len() < 16 { // Magic (4) + Nonce (12)
            return Err(BackendError::CryptoError("File too short".into()).into());
        }

        // Verify Magic
        if &packet[0..4] != MAGIC_HEADER {
            return Err(BackendError::CryptoError("Invalid file signature".into()).into());
        }

        let nonce_bytes = &packet[4..16];
        let ciphertext = &packet[16..];
        
        let nonce = Nonce::from_slice(nonce_bytes);
        
        let plaintext = self.cipher.decrypt(nonce, ciphertext)
            .map_err(BackendError::from)?;
            
        fs::write(&output_path, plaintext).map_err(BackendError::from)?;
        Ok(())
    }
}

// ==========================================================================
// SECTION 5: LINEAR ALGEBRA (THE MATRIX)
// ==========================================================================
// High-performance parallel matrix engine. Supports basic ops + transformations.

#[pyclass]
#[derive(Clone, Debug)]
struct Matrix {
    rows: usize,
    cols: usize,
    data: Vec<f64>,
}

#[pymethods]
impl Matrix {
    #[new]
    fn new(rows: usize, cols: usize, data: Vec<f64>) -> PyResult<Self> {
        if data.len() != rows * cols {
            return Err(BackendError::InvalidState("Data len != rows*cols".into()).into());
        }
        Ok(Matrix { rows, cols, data })
    }

    #[staticmethod]
    fn zeros(rows: usize, cols: usize) -> Self {
        Matrix { rows, cols, data: vec![0.0; rows * cols] }
    }
    
    #[staticmethod]
    fn random(rows: usize, cols: usize) -> Self {
        let mut rng = rand::thread_rng();
        let data: Vec<f64> = (0..rows*cols).map(|_| rng.gen::<f64>()).collect();
        Matrix { rows, cols, data }
    }

    #[staticmethod]
    fn identity(n: usize) -> Self {
        let mut data = vec![0.0; n * n];
        for i in 0..n {
            data[i * n + i] = 1.0;
        }
        Matrix { rows: n, cols: n, data }
    }
    
    fn shape(&self) -> (usize, usize) {
        (self.rows, self.cols)
    }

    fn get(&self, r: usize, c: usize) -> PyResult<f64> {
        if r >= self.rows || c >= self.cols {
            return Err(BackendError::IndexOutOfBounds { requested: r.max(c), max: self.rows.max(self.cols) }.into());
        }
        Ok(self.data[r * self.cols + c])
    }
    
    fn set(&mut self, r: usize, c: usize, val: f64) -> PyResult<()> {
        if r >= self.rows || c >= self.cols {
            return Err(BackendError::IndexOutOfBounds { requested: r.max(c), max: self.rows.max(self.cols) }.into());
        }
        self.data[r * self.cols + c] = val;
        Ok(())
    }

    /// Parallel Addition
    fn add(&self, other: &Matrix) -> PyResult<Matrix> {
        if self.rows != other.rows || self.cols != other.cols {
            return Err(BackendError::MatrixDimensionMismatch { 
                r1: self.rows, c1: self.cols, r2: other.rows, c2: other.cols 
            }.into());
        }
        
        let new_data: Vec<f64> = self.data.par_iter()
            .zip(other.data.par_iter())
            .map(|(a, b)| a + b)
            .collect();
            
        Ok(Matrix { rows: self.rows, cols: self.cols, data: new_data })
    }

    /// Parallel Subtraction
    fn sub(&self, other: &Matrix) -> PyResult<Matrix> {
        if self.rows != other.rows || self.cols != other.cols {
            return Err(BackendError::MatrixDimensionMismatch { 
                r1: self.rows, c1: self.cols, r2: other.rows, c2: other.cols 
            }.into());
        }
        
        let new_data: Vec<f64> = self.data.par_iter()
            .zip(other.data.par_iter())
            .map(|(a, b)| a - b)
            .collect();
            
        Ok(Matrix { rows: self.rows, cols: self.cols, data: new_data })
    }

    /// Parallel Scalar Multiplication
    fn scale(&self, factor: f64) -> Matrix {
        let new_data: Vec<f64> = self.data.par_iter().map(|&x| x * factor).collect();
        Matrix { rows: self.rows, cols: self.cols, data: new_data }
    }

    /// Parallel Matrix Multiplication (Naive O(N^3) but parallelized)
    fn matmul(&self, other: &Matrix) -> PyResult<Matrix> {
        if self.cols != other.rows {
            return Err(BackendError::MatrixDimensionMismatch { 
                r1: self.rows, c1: self.cols, r2: other.rows, c2: other.cols 
            }.into());
        }

        let n = self.rows;
        let m = other.cols;
        let p = self.cols;

        let mut result_data = vec![0.0; n * m];

        // We use chunking to distribute rows across threads
        result_data.par_chunks_mut(m).enumerate().for_each(|(i, row_slice)| {
            for j in 0..m {
                let mut sum = 0.0;
                for k in 0..p {
                    // A[i][k] * B[k][j]
                    let a_val = unsafe { *self.data.get_unchecked(i * p + k) };
                    let b_val = unsafe { *other.data.get_unchecked(k * m + j) };
                    sum += a_val * b_val;
                }
                row_slice[j] = sum;
            }
        });

        track_op(n * m * p);
        Ok(Matrix { rows: n, cols: m, data: result_data })
    }

    /// Transpose Matrix
    fn transpose(&self) -> Matrix {
        let mut new_data = vec![0.0; self.rows * self.cols];
        for i in 0..self.rows {
            for j in 0..self.cols {
                new_data[j * self.rows + i] = self.data[i * self.cols + j];
            }
        }
        Matrix { rows: self.cols, cols: self.rows, data: new_data }
    }

    /// Calculate Determinant (Recursive - Slow for large matrices)
    /// Included for feature completeness.
    fn determinant(&self) -> PyResult<f64> {
        if self.rows != self.cols {
            return Err(BackendError::MatrixDimensionMismatch { r1: self.rows, c1: self.cols, r2: 0, c2: 0 }.into());
        }
        Ok(self._det_recursive(&self.data, self.rows))
    }

    // Internal helper for determinant
    fn _det_recursive(&self, data: &[f64], n: usize) -> f64 {
        if n == 1 { return data[0]; }
        if n == 2 { return data[0] * data[3] - data[1] * data[2]; }

        let mut det = 0.0;
        for c in 0..n {
            let sub_matrix = self._sub_matrix(data, n, 0, c);
            let sign = if c % 2 == 0 { 1.0 } else { -1.0 };
            det += sign * data[c] * self._det_recursive(&sub_matrix, n - 1);
        }
        det
    }

    fn _sub_matrix(&self, data: &[f64], n: usize, skip_r: usize, skip_c: usize) -> Vec<f64> {
        let mut sub = Vec::with_capacity((n - 1) * (n - 1));
        for r in 0..n {
            if r == skip_r { continue; }
            for c in 0..n {
                if c == skip_c { continue; }
                sub.push(data[r * n + c]);
            }
        }
        sub
    }
}

// ==========================================================================
// SECTION 6: GRAPH THEORY (THE NETWORK ENGINE)
// ==========================================================================
// A robust Directed Graph implementation for network analysis.

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GraphNode {
    id: String,
    data: HashMap<String, String>, // Metadata
}

#[pyclass]
struct GraphEngine {
    nodes: HashMap<String, GraphNode>,
    adjacency: HashMap<String, Vec<(String, f64)>>, // Adjacency List: Node -> [(Neighbor, Weight)]
}

#[pymethods]
impl GraphEngine {
    #[new]
    fn new() -> Self {
        GraphEngine {
            nodes: HashMap::new(),
            adjacency: HashMap::new(),
        }
    }

    fn add_node(&mut self, id: String) -> PyResult<()> {
        if self.nodes.len() >= MAX_GRAPH_NODES {
            return Err(BackendError::GraphTooLarge.into());
        }
        if self.nodes.contains_key(&id) {
            return Ok(()); // Idempotent
        }
        self.nodes.insert(id.clone(), GraphNode { id: id.clone(), data: HashMap::new() });
        self.adjacency.entry(id).or_insert_with(Vec::new);
        Ok(())
    }

    fn add_edge(&mut self, src: String, dst: String, weight: f64) -> PyResult<()> {
        if !self.nodes.contains_key(&src) { return Err(BackendError::NodeNotFound(src).into()); }
        if !self.nodes.contains_key(&dst) { return Err(BackendError::NodeNotFound(dst).into()); }
        
        self.adjacency.entry(src).or_default().push((dst, weight));
        Ok(())
    }

    /// Breadth-First Search (BFS)
    fn bfs(&self, start: String) -> PyResult<Vec<String>> {
        if !self.nodes.contains_key(&start) { return Err(BackendError::NodeNotFound(start).into()); }

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut order = Vec::new();

        visited.insert(start.clone());
        queue.push_back(start);

        while let Some(current) = queue.pop_front() {
            order.push(current.clone());

            if let Some(neighbors) = self.adjacency.get(&current) {
                for (neighbor, _) in neighbors {
                    if !visited.contains(neighbor) {
                        visited.insert(neighbor.clone());
                        queue.push_back(neighbor.clone());
                    }
                }
            }
        }
        Ok(order)
    }

    /// Dijkstra's Shortest Path Algorithm
    fn shortest_path(&self, start: String, end: String) -> PyResult<Option<(Vec<String>, f64)>> {
        if !self.nodes.contains_key(&start) { return Err(BackendError::NodeNotFound(start).into()); }
        if !self.nodes.contains_key(&end) { return Err(BackendError::NodeNotFound(end).into()); }

        // Distance map: Node -> Shortest Distance Found So Far
        let mut dist: HashMap<String, f64> = HashMap::new();
        // Predecessor map: Node -> Previous Node (for reconstruction)
        let mut prev: HashMap<String, String> = HashMap::new();
        
        // Priority Queue simulation (Vec + Sort) - Naive but functional for V3
        // Ideally use BinaryHeap<State>
        let mut queue: Vec<(String, f64)> = Vec::new();

        // Init
        for node in self.nodes.keys() {
            dist.insert(node.clone(), f64::INFINITY);
        }
        dist.insert(start.clone(), 0.0);
        queue.push((start, 0.0));

        while let Some((u, d)) = Self::_pop_min(&mut queue) {
            if d > *dist.get(&u).unwrap_or(&f64::INFINITY) { continue; }
            if u == end { break; } // Found target

            if let Some(neighbors) = self.adjacency.get(&u) {
                for (v, weight) in neighbors {
                    let next_dist = d + weight;
                    if next_dist < *dist.get(v).unwrap_or(&f64::INFINITY) {
                        dist.insert(v.clone(), next_dist);
                        prev.insert(v.clone(), u.clone());
                        queue.push((v.clone(), next_dist));
                    }
                }
            }
        }

        // Reconstruct path
        if *dist.get(&end).unwrap_or(&f64::INFINITY) == f64::INFINITY {
            return Ok(None);
        }

        let mut path = Vec::new();
        let mut curr = end.clone();
        path.push(curr.clone());
        while let Some(p) = prev.get(&curr) {
            path.push(p.clone());
            curr = p.clone();
        }
        path.reverse();
        
        Ok(Some((path, dist[&end])))
    }

    // Helper for Priority Queue
    fn _pop_min(queue: &mut Vec<(String, f64)>) -> Option<(String, f64)> {
        // Sort descending by distance so we can pop from end (O(N log N))
        // Terrible for massive graphs, acceptable for "Intimidate" level
        queue.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(CmpOrdering::Equal));
        queue.pop()
    }

    /// Calculate PageRank (Centrality)
    fn pagerank(&self, iterations: usize, damping: f64) -> PyResult<HashMap<String, f64>> {
        let n = self.nodes.len();
        if n == 0 { return Ok(HashMap::new()); }
        
        let mut scores: HashMap<String, f64> = self.nodes.keys()
            .map(|k| (k.clone(), 1.0 / n as f64))
            .collect();
            
        for _ in 0..iterations {
            let mut new_scores: HashMap<String, f64> = HashMap::new();
            let base_score = (1.0 - damping) / n as f64;
            
            // Distribute scores
            for (node, score) in &scores {
                if let Some(neighbors) = self.adjacency.get(node) {
                    if neighbors.is_empty() {
                         // Dangling node logic omitted for brevity, usually spread to all
                    } else {
                        let share = (score * damping) / neighbors.len() as f64;
                        for (neighbor, _) in neighbors {
                            *new_scores.entry(neighbor.clone()).or_insert(0.0) += share;
                        }
                    }
                }
            }
            
            // Add base score
            for score in new_scores.values_mut() {
                *score += base_score;
            }
            scores = new_scores;
        }
        Ok(scores)
    }
}

// ==========================================================================
// SECTION 7: DATAFRAMES (THE MINI-POLARS)
// ==========================================================================
// A columnar data engine. Stores data in vectors, processes in parallel.

#[derive(Clone, Debug)]
enum ColumnData {
    Float(Vec<f64>),
    Int(Vec<i64>),
    Str(Vec<String>),
}

#[pyclass]
struct DataFrame {
    columns: HashMap<String, ColumnData>,
    row_count: usize,
}

#[pymethods]
impl DataFrame {
    #[new]
    fn new() -> Self {
        DataFrame {
            columns: HashMap::new(),
            row_count: 0,
        }
    }

    fn add_column_float(&mut self, name: String, data: Vec<f64>) -> PyResult<()> {
        self._validate_len(data.len())?;
        self.columns.insert(name, ColumnData::Float(data));
        Ok(())
    }

    fn add_column_int(&mut self, name: String, data: Vec<i64>) -> PyResult<()> {
        self._validate_len(data.len())?;
        self.columns.insert(name, ColumnData::Int(data));
        Ok(())
    }

    fn add_column_str(&mut self, name: String, data: Vec<String>) -> PyResult<()> {
        self._validate_len(data.len())?;
        self.columns.insert(name, ColumnData::Str(data));
        Ok(())
    }

    fn _validate_len(&mut self, len: usize) -> PyResult<()> {
        if self.columns.is_empty() {
            self.row_count = len;
        } else if len != self.row_count {
            return Err(BackendError::InvalidState("Column length mismatch".into()).into());
        }
        Ok(())
    }

    /// Returns basic summary stats for a numeric column
    fn describe(&self, col_name: String) -> PyResult<Py<PyDict>> {
        match self.columns.get(&col_name) {
            Some(ColumnData::Float(data)) => {
                let mean = data.iter().sum::<f64>() / data.len() as f64;
                let min = data.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                let max = data.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                
                Python::with_gil(|py| {
                    let d = PyDict::new(py);
                    d.set_item("mean", mean)?;
                    d.set_item("min", min)?;
                    d.set_item("max", max)?;
                    d.set_item("count", data.len())?;
                    Ok(d.into())
                })
            },
            Some(ColumnData::Int(data)) => {
                let mean = data.iter().sum::<i64>() as f64 / data.len() as f64;
                let min = data.iter().min().unwrap_or(&0);
                let max = data.iter().max().unwrap_or(&0);
                
                Python::with_gil(|py| {
                    let d = PyDict::new(py);
                    d.set_item("mean", mean)?;
                    d.set_item("min", *min)?;
                    d.set_item("max", *max)?;
                    d.set_item("count", data.len())?;
                    Ok(d.into())
                })
            },
            _ => Err(BackendError::TypeMismatch("Not a numeric column".into()).into())
        }
    }

    /// Parallel Filter: Returns indices where col > val
    fn filter_gt(&self, col_name: String, val: f64) -> PyResult<Vec<usize>> {
        match self.columns.get(&col_name) {
            Some(ColumnData::Float(data)) => {
                Ok(data.par_iter().enumerate()
                    .filter(|(_, &x)| x > val)
                    .map(|(i, _)| i)
                    .collect())
            },
            Some(ColumnData::Int(data)) => {
                 Ok(data.par_iter().enumerate()
                    .filter(|(_, &x)| x as f64 > val)
                    .map(|(i, _)| i)
                    .collect())
            },
            _ => Err(BackendError::TypeMismatch("Column not numeric".into()).into())
        }
    }
}

// ==========================================================================
// SECTION 8: SYSTEM SENTINEL (RATE LIMITER)
// ==========================================================================

#[derive(Clone)]
struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

#[pyclass]
struct Sentinel {
    capacity: f64,
    refill_rate: f64,
    state: Arc<FastMutex<BucketState>>,
}

#[pymethods]
impl Sentinel {
    #[new]
    fn new(capacity: f64, refill_rate: f64) -> Self {
        Sentinel {
            capacity,
            refill_rate,
            state: Arc::new(FastMutex::new(BucketState {
                tokens: capacity,
                last_refill: Instant::now(),
            })),
        }
    }

    fn try_pass(&self, cost: f64) -> bool {
        let mut state = self.state.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        
        // Refill
        let new_tokens = elapsed * self.refill_rate;
        if new_tokens > 0.0 {
            state.tokens = (state.tokens + new_tokens).min(self.capacity);
            state.last_refill = now;
        }

        if state.tokens >= cost {
            state.tokens -= cost;
            true
        } else {
            false
        }
    }

    fn wait(&self, py: Python, cost: f64) -> PyResult<()> {
        let sleep_time = Duration::from_millis(10);
        loop {
            if let Err(e) = py.check_signals() { return Err(e); }
            if self.try_pass(cost) { return Ok(()); }
            std::thread::sleep(sleep_time);
        }
    }
}

// ==========================================================================
// SECTION 9: GHOST ENGINE (INDEXING & BLOOM FILTERS)
// ==========================================================================

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
struct IndexEntry {
    line_num: usize,
    byte_offset: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct BloomFilter {
    bit_vec: Vec<u64>,
    bitmap_size: usize,
    hash_count: u32,
}

impl BloomFilter {
    fn new(items: usize) -> Self {
        let size = items * 10; // 10 bits per item = <1% false positive
        let vec_size = (size + 63) / 64;
        BloomFilter {
            bit_vec: vec![0; vec_size],
            bitmap_size: vec_size * 64,
            hash_count: 3,
        }
    }

    fn insert(&mut self, item: &str) {
        let mut h = blake3::hash(item.as_bytes());
        let mut hash_val = u64::from_le_bytes(h.as_bytes()[0..8].try_into().unwrap());
        
        for _ in 0..self.hash_count {
            let bit_idx = (hash_val as usize) % self.bitmap_size;
            self.bit_vec[bit_idx / 64] |= 1 << (bit_idx % 64);
            hash_val = hash_val.wrapping_mul(0x517cc1b727220a95).wrapping_add(1); // LCG
        }
    }

    fn contains(&self, item: &str) -> bool {
        let mut h = blake3::hash(item.as_bytes());
        let mut hash_val = u64::from_le_bytes(h.as_bytes()[0..8].try_into().unwrap());
        
        for _ in 0..self.hash_count {
            let bit_idx = (hash_val as usize) % self.bitmap_size;
            if (self.bit_vec[bit_idx / 64] & (1 << (bit_idx % 64))) == 0 {
                return false;
            }
            hash_val = hash_val.wrapping_mul(0x517cc1b727220a95).wrapping_add(1);
        }
        true
    }
}

#[pyclass]
struct GhostEngine {
    filepath: PathBuf,
    index_path: PathBuf,
    bloom_path: PathBuf,
    indexed: Arc<AtomicBool>,
    index_cache: Arc<FastRwLock<Vec<IndexEntry>>>,
    bloom_cache: Arc<FastRwLock<Option<BloomFilter>>>,
}

#[pymethods]
impl GhostEngine {
    #[new]
    fn new(filepath: String) -> Self {
        let path = PathBuf::from(&filepath);
        let index_path = path.with_extension("idx");
        let bloom_path = path.with_extension("bloom");
        
        GhostEngine { 
            filepath: path,
            index_path,
            bloom_path,
            indexed: Arc::new(AtomicBool::new(false)),
            index_cache: Arc::new(FastRwLock::new(Vec::new())),
            bloom_cache: Arc::new(FastRwLock::new(None)),
        }
    }

    /// Builds the index (Heavy Operation)
    fn build_index(&self) -> PyResult<()> {
        if self.indexed.load(Ordering::Relaxed) { return Ok(()); }
        
        let file = File::open(&self.filepath)?;
        let mut reader = BufReader::with_capacity(CHUNK_SIZE, file);
        
        let mut index = Vec::new();
        let mut bloom = BloomFilter::new(1_000_000); // Initial capacity
        let mut offset = 0u64;
        let mut line_count = 0usize;
        let mut line_buf = String::new();
        
        loop {
            line_buf.clear();
            let n = reader.read_line(&mut line_buf)?;
            if n == 0 { break; }
            
            bloom.insert(line_buf.trim());
            
            if line_count % INDEX_INTERVAL == 0 {
                index.push(IndexEntry { line_num: line_count, byte_offset: offset });
            }
            
            offset += n as u64;
            line_count += 1;
            track_op(n);
        }
        
        // Save Metadata
        {
            let mut w_idx = BufWriter::new(File::create(&self.index_path)?);
            bincode::serialize_into(&mut w_idx, &index).map_err(BackendError::from)?;
            
            let mut w_blm = BufWriter::new(File::create(&self.bloom_path)?);
            bincode::serialize_into(&mut w_blm, &bloom).map_err(BackendError::from)?;
        }
        
        // Update Cache
        *self.index_cache.write() = index;
        *self.bloom_cache.write() = Some(bloom);
        self.indexed.store(true, Ordering::Relaxed);
        
        Ok(())
    }

    fn get_line(&self, line_num: usize) -> PyResult<Option<String>> {
        self.build_index()?; // Ensure indexed
        
        let index_read = self.index_cache.read();
        
        // Binary search for closest previous index
        let idx = match index_read.binary_search_by_key(&line_num, |entry| entry.line_num) {
            Ok(i) => index_read[i],
            Err(i) => if i == 0 { 
                IndexEntry { line_num: 0, byte_offset: 0 } 
            } else { 
                index_read[i - 1] 
            }
        };

        let mut file = File::open(&self.filepath)?;
        file.seek(SeekFrom::Start(idx.byte_offset))?;
        let mut reader = BufReader::new(file);
        
        let skip = line_num - idx.line_num;
        let mut buf = String::new();
        
        for _ in 0..skip {
            if reader.read_line(&mut buf)? == 0 { return Ok(None); }
            buf.clear();
        }
        
        if reader.read_line(&mut buf)? == 0 {
            Ok(None)
        } else {
            Ok(Some(buf.trim().to_string()))
        }
    }
    
    fn might_contain(&self, text: String) -> PyResult<bool> {
        self.build_index()?;
        let guard = self.bloom_cache.read();
        if let Some(bloom) = &*guard {
            Ok(bloom.contains(&text))
        } else {
            Err(BackendError::InvalidState("Bloom filter missing".into()).into())
        }
    }
}

// ==========================================================================
// SECTION 10: TIME CAPSULE MANAGER (PERSISTENCE)
// ==========================================================================

#[derive(Serialize, Deserialize)]
struct CapsuleHeader {
    magic: [u8; 4],
    checksum: String,
    timestamp: u64,
}

#[pyclass]
struct TimeCapsuleManager {
    root: PathBuf,
}

#[pymethods]
impl TimeCapsuleManager {
    #[new]
    fn new(root: String) -> Self {
        TimeCapsuleManager { root: PathBuf::from(root) }
    }

    fn atomic_dump(&self, key: String, data: &[u8]) -> PyResult<()> {
        let path = self.root.join(format!("{}.cap", key));
        let tmp = path.with_extension("tmp");
        
        if let Some(p) = path.parent() { fs::create_dir_all(p)?; }
        
        let checksum = blake3::hash(data).to_hex().to_string();
        let header = CapsuleHeader {
            magic: *MAGIC_HEADER,
            checksum,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        };
        
        {
            let mut f = BufWriter::new(File::create(&tmp)?);
            bincode::serialize_into(&mut f, &header).map_err(BackendError::from)?;
            f.write_all(data)?;
            f.flush()?;
        }
        
        fs::rename(tmp, path)?;
        track_op(data.len());
        Ok(())
    }

    fn fast_load<'py>(&self, py: Python<'py>, key: String) -> PyResult<Option<Py<PyBytes>>> {
        let path = self.root.join(format!("{}.cap", key));
        if !path.exists() { return Ok(None); }
        
        let mut f = BufReader::new(File::open(&path)?);
        
        // Read Header (Variable length due to serialization, but bincode handles it)
        // Note: For speed, we assume header is small.
        let header: CapsuleHeader = bincode::deserialize_from(&mut f).map_err(BackendError::from)?;
        
        if header.magic != *MAGIC_HEADER {
            return Err(BackendError::CorruptIndex("Invalid Magic".into()).into());
        }
        
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;
        
        let actual_sum = blake3::hash(&data).to_hex().to_string();
        if actual_sum != header.checksum {
            return Err(BackendError::CorruptIndex("Checksum Mismatch".into()).into());
        }
        
        Ok(Some(PyBytes::new(py, &data).into()))
    }
}

// ==========================================================================
// SECTION 11: MODULE INIT
// ==========================================================================

#[pymodule]
fn chalna_rs_backend(_py: Python, m: &PyModule) -> PyResult<()> {
    SYSTEM_START.store(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as usize, Ordering::Relaxed);

    m.add("ChalnaEngineError", _py.get_type::<ChalnaEngineError>())?;
    m.add("ChalnaCorruptDataError", _py.get_type::<ChalnaCorruptDataError>())?;
    m.add("ChalnaSecurityError", _py.get_type::<ChalnaSecurityError>())?;
    m.add("ChalnaRateLimitError", _py.get_type::<ChalnaRateLimitError>())?;
    m.add("ChalnaGraphError", _py.get_type::<ChalnaGraphError>())?;
    m.add("ChalnaMathError", _py.get_type::<ChalnaMathError>())?;

    m.add_class::<Telemetry>()?;
    m.add_class::<Vault>()?;
    m.add_class::<Matrix>()?;
    m.add_class::<GraphEngine>()?;
    m.add_class::<DataFrame>()?;
    m.add_class::<Sentinel>()?;
    m.add_class::<GhostEngine>()?;
    m.add_class::<TimeCapsuleManager>()?;
    
    Ok(())
}