import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent

def find_test_dirs():
    return [
        str(p)
        for p in ROOT.rglob("tests")
        if p.is_dir()
        and ".git" not in p.parts
        and ".venv" not in p.parts
    ]

def main():
    lint = subprocess.run([sys.executable, "-m", "ruff", "check", str(ROOT), "--fix"], check=False)
    if lint.returncode != 0:
        print("Linting failed (fix errors before running tests)")
        sys.exit(lint.returncode)

    test_dirs = find_test_dirs()

    if not test_dirs:
        print("No test directories found.")
        sys.exit(1)

    print("Running tests in:")
    for d in test_dirs:
        print(f"  {d}")

    result = subprocess.run(
        [sys.executable, "-m", "pytest", *test_dirs, "-v", "--tb=short"],
        cwd=ROOT,
        check=False,
    )

    sys.exit(result.returncode)

if __name__ == "__main__":
    main()