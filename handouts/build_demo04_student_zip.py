"""Build the Demo 04 student ZIP from the published handout sources.

The published files in ``handouts/`` are the only executable source of truth.
This script packages those files without maintaining a second source tree.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from pathlib import PurePosixPath
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


HANDOUTS_DIR = Path(__file__).resolve().parent
OUTPUT_PATH = HANDOUTS_DIR / "demo04-student.zip"
PACKAGE_ROOT = "demo04-student"
ARCHIVE_TIMESTAMP = (2026, 7, 16, 0, 0, 0)
SECRET_ENV_KEYS = (
    "BOOTSTRAP_SERVERS",
    "SASL_USERNAME",
    "SASL_PASSWORD",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
)

SOURCE_MAP: dict[str, Path] = {
    "requirements.txt": HANDOUTS_DIR / "requirements.txt",
    ".env.example": HANDOUTS_DIR / ".env.example",
    "demo04_common.py": HANDOUTS_DIR / "demo04_common.py",
    "demo04a_schema_validation.py": HANDOUTS_DIR / "demo04a_schema_validation.py",
    "demo04b_local_avro_roundtrip.py": HANDOUTS_DIR
    / "demo04b_local_avro_roundtrip.py",
    "demo04c_confluent_avro_roundtrip.py": HANDOUTS_DIR
    / "demo04c_confluent_avro_roundtrip.py",
    "demo04d_asyncio_avro_roundtrip.py": HANDOUTS_DIR
    / "demo04d_asyncio_avro_roundtrip.py",
    "trip_event_v1.avsc": HANDOUTS_DIR / "trip_event_v1.avsc",
    "trip_event_v2_reader.avsc": HANDOUTS_DIR / "trip_event_v2_reader.avsc",
    "tests/conftest.py": HANDOUTS_DIR / "demo04-tests" / "conftest.py",
    "tests/test_demo04_local.py": HANDOUTS_DIR
    / "demo04-tests"
    / "test_demo04_local.py",
}

STUDENT_GITIGNORE = """# Local credentials and credential variants
.env
.env.*
!.env.example

# Local environments
.venv/
venv/

# Demo evidence and generated output
outputs/

# Python and test caches
__pycache__/
*.py[cod]
.pytest_cache/
.mypy_cache/
.ruff_cache/

# macOS metadata
.DS_Store
"""


def _zip_info(name: str, *, is_dir: bool = False) -> ZipInfo:
    """Return deterministic ZIP metadata with ordinary Unix permissions."""

    normalized = name.rstrip("/") + ("/" if is_dir else "")
    info = ZipInfo(normalized, ARCHIVE_TIMESTAMP)
    info.create_system = 3
    info.external_attr = ((0o755 if is_dir else 0o644) & 0xFFFF) << 16
    info.compress_type = ZIP_DEFLATED
    return info


def _validate_package_inputs() -> None:
    """Reject missing sources, unsafe archive names, or populated credentials."""

    missing = [str(path) for path in SOURCE_MAP.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Missing published Demo 04 source files: {missing}")

    unsafe_names = [
        name
        for name in SOURCE_MAP
        if PurePosixPath(name).is_absolute() or ".." in PurePosixPath(name).parts
    ]
    if unsafe_names:
        raise ValueError(f"Unsafe student ZIP paths: {unsafe_names}")

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


def build_student_zip(output_path: Path = OUTPUT_PATH) -> Path:
    """Create the bounded student package directly from published sources."""

    _validate_package_inputs()
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with ZipFile(output_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(_zip_info(PACKAGE_ROOT, is_dir=True), b"")
        archive.writestr(
            _zip_info(f"{PACKAGE_ROOT}/tests", is_dir=True),
            b"",
        )
        archive.writestr(
            _zip_info(f"{PACKAGE_ROOT}/.gitignore"),
            STUDENT_GITIGNORE.encode("utf-8"),
        )
        for archive_name, source_path in SOURCE_MAP.items():
            archive.writestr(
                _zip_info(f"{PACKAGE_ROOT}/{archive_name}"),
                source_path.read_bytes(),
            )

    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_PATH,
        help="ZIP output path (default: handouts/demo04-student.zip)",
    )
    args = parser.parse_args()
    built = build_student_zip(args.output)
    print(f"Built {built}")
