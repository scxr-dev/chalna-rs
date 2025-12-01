import time
import chalna_rs as crs
import statistics

def standard_python_math(n):
    return [x**2 for x in range(n)]

def main():
    N = 5_000_000
    print(f"🏎️  RACE START: Processing {N:,} items...")

    # 1. Python Baseline
    start = time.perf_counter()
    _ = standard_python_math(N)
    py_time = time.perf_counter() - start
    print(f"🐍 Python List Comp : {py_time:.4f}s")

    # 2. Chalna Turbo
    start = time.perf_counter()
    # Using a lambda here for fairness, but Rust backend optimizes map overhead
    _ = crs.Turbo.map(lambda x: x**2, range(N), desc="Turbo Bench")
    rs_time = time.perf_counter() - start
    print(f"🦀 Chalna Turbo    : {rs_time:.4f}s")

    speedup = py_time / rs_time
    print(f"\n🚀 SPEEDUP FACTOR: {speedup:.2f}x FASTER")
    
    if speedup > 1.0:
        print("✅ Chalna-RS wins!")
    else:
        print("⚠️  Warning: Overhead is high for simple math. Try heavier functions.")

if __name__ == "__main__":
    main()