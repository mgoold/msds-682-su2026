"""Build Demo 07 student ZIP directly from published handout sources."""

from __future__ import annotations

import argparse
from pathlib import Path, PurePosixPath
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

HANDOUTS_DIR = Path(__file__).resolve().parent
OUTPUT_PATH = HANDOUTS_DIR / "demo07-student.zip"
PACKAGE_ROOT = "demo07-student"
ARCHIVE_TIMESTAMP = (2026, 7, 23, 0, 0, 0)
PACKAGE_DIRECTORIES = ("tests",)
SECRET_ENV_KEYS = (
    "BOOTSTRAP_SERVERS",
    "SASL_USERNAME",
    "SASL_PASSWORD",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
)

SOURCE_MAP: dict[str, Path] = {
    "README.md": HANDOUTS_DIR / "demo07.md",
    "requirements.txt": HANDOUTS_DIR / "requirements.txt",
    ".env.example": HANDOUTS_DIR / ".env.example",
    "confluent_demo_common.py": HANDOUTS_DIR / "confluent_demo_common.py",
    "demo07_common.py": HANDOUTS_DIR / "demo07_common.py",
    "demo07_kafka.py": HANDOUTS_DIR / "demo07_kafka.py",
    "demo07_processor_runtime.py": HANDOUTS_DIR / "demo07_processor_runtime.py",
    "demo07_routing.py": HANDOUTS_DIR / "demo07_routing.py",
    "demo07_trip_request_v1.avsc": HANDOUTS_DIR
    / "demo07_trip_request_v1.avsc",
    "demo07_fare_quote_v1.avsc": HANDOUTS_DIR / "demo07_fare_quote_v1.avsc",
    "demo07_trip_outcome_v1.avsc": HANDOUTS_DIR
    / "demo07_trip_outcome_v1.avsc",
    "demo07_pricing_evaluation_v1.avsc": HANDOUTS_DIR
    / "demo07_pricing_evaluation_v1.avsc",
    "demo07a_train_cost_model.py": HANDOUTS_DIR
    / "demo07a_train_cost_model.py",
    "demo07b_produce_trip_requests.py": HANDOUTS_DIR
    / "demo07b_produce_trip_requests.py",
    "demo07c_confluent_fare_quote_processor.py": HANDOUTS_DIR
    / "demo07c_confluent_fare_quote_processor.py",
    "demo07d_produce_trip_outcomes.py": HANDOUTS_DIR
    / "demo07d_produce_trip_outcomes.py",
    "demo07e_confluent_quote_outcome_evaluator.py": HANDOUTS_DIR
    / "demo07e_confluent_quote_outcome_evaluator.py",
    "demo07f_compare_models.py": HANDOUTS_DIR / "demo07f_compare_models.py",
    "tests/conftest.py": HANDOUTS_DIR / "demo07-tests" / "conftest.py",
    "tests/test_demo07_local.py": HANDOUTS_DIR
    / "demo07-tests"
    / "test_demo07_local.py",
}

STUDENT_GITIGNORE = """# Credentials
.env
.env.*
!.env.example

# Environments, generated artifacts, and evidence
.venv/
venv/
outputs/

# Caches and local metadata
__pycache__/
*.py[cod]
.pytest_cache/
.mypy_cache/
.ruff_cache/
.DS_Store
"""

README_REPLACEMENTS = {
    "- [Download `demo07-student.zip`](handouts/demo07-student.zip)": (
        "- This extracted package already contains all Demo 07 student files."
    ),
}


def _zip_info(name: str, *, is_dir: bool = False) -> ZipInfo:
    normalized = name.rstrip("/") + ("/" if is_dir else "")
    info = ZipInfo(normalized, ARCHIVE_TIMESTAMP)
    info.create_system = 3
    info.external_attr = ((0o755 if is_dir else 0o644) & 0xFFFF) << 16
    info.compress_type = ZIP_DEFLATED
    return info


def _validate_package_inputs() -> None:
    missing = [str(path) for path in SOURCE_MAP.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Missing Demo 07 source files: {missing}")
    unsafe = [
        name
        for name in SOURCE_MAP
        if PurePosixPath(name).is_absolute() or ".." in PurePosixPath(name).parts
    ]
    if unsafe:
        raise ValueError(f"Unsafe student ZIP paths: {unsafe}")
    env_rows = dict(
        line.split("=", 1)
        for line in SOURCE_MAP[".env.example"].read_text(encoding="utf-8").splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    )
    populated = [key for key in SECRET_ENV_KEYS if env_rows.get(key, "").strip()]
    if populated:
        raise ValueError(
            "Refusing to package populated credential fields: " + ", ".join(populated)
        )


def _source_bytes(archive_name: str, source_path: Path) -> bytes:
    if archive_name != "README.md":
        return source_path.read_bytes()
    content = source_path.read_text(encoding="utf-8")
    for website_text, package_text in README_REPLACEMENTS.items():
        if website_text not in content:
            raise ValueError(f"Missing expected README text: {website_text}")
        content = content.replace(website_text, package_text)
    return content.encode("utf-8")


def build_student_zip(output_path: Path = OUTPUT_PATH) -> Path:
    """Create one deterministic ZIP without a duplicate source tree."""

    _validate_package_inputs()
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(_zip_info(PACKAGE_ROOT, is_dir=True), b"")
        for directory in PACKAGE_DIRECTORIES:
            archive.writestr(
                _zip_info(f"{PACKAGE_ROOT}/{directory}", is_dir=True),
                b"",
            )
        archive.writestr(
            _zip_info(f"{PACKAGE_ROOT}/.gitignore"),
            STUDENT_GITIGNORE.encode("utf-8"),
        )
        for archive_name, source_path in SOURCE_MAP.items():
            archive.writestr(
                _zip_info(f"{PACKAGE_ROOT}/{archive_name}"),
                _source_bytes(archive_name, source_path),
            )
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()
    print(f"Built {build_student_zip(args.output)}")
