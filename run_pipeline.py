from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from movielens_pipeline.orchestrator import run_project_pipeline


def main():
    summary = run_project_pipeline(PROJECT_ROOT)
    print(f"Pipeline summary: {summary}")


if __name__ == "__main__":
    main()
