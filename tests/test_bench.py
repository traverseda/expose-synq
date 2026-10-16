"""Keeps bench/bench.py runnable. It doesn't check numbers; run the bench for those."""
import importlib.util
from pathlib import Path


def test_quick_bench_runs():
    path = Path(__file__).parent.parent / "bench" / "bench.py"
    spec = importlib.util.spec_from_file_location("bench", path)
    bench = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bench)
    results = bench.main(["--quick"])
    assert results
    assert [r.line() for r in results if r.error and r.error != "not implemented"] == []
