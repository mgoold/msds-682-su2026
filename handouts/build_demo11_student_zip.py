"""Build Demo 11 student ZIP from the published SSOT sources."""

from __future__ import annotations

import argparse
from pathlib import Path, PurePosixPath
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


HANDOUTS_DIR = Path(__file__).resolve().parent
OUTPUT_PATH = HANDOUTS_DIR / "demo11-student.zip"
PACKAGE_ROOT = "demo11-student"
ARCHIVE_TIMESTAMP = (2026, 8, 9, 0, 0, 0)
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
    "README.md": HANDOUTS_DIR / "demo11.md",
    "requirements.txt": HANDOUTS_DIR / "requirements.txt",
    ".env.example": HANDOUTS_DIR / ".env.example",
    "confluent_demo_common.py": HANDOUTS_DIR / "confluent_demo_common.py",
    "trip_event_contract.py": HANDOUTS_DIR / "trip_event_contract.py",
    "trip_event_v1.avsc": HANDOUTS_DIR / "trip_event_v1.avsc",
    "demo05_common.py": HANDOUTS_DIR / "demo05_common.py",
    "demo05_app.py": HANDOUTS_DIR / "demo05_app.py",
    "demo05_kafka.py": HANDOUTS_DIR / "demo05_kafka.py",
    "demo11_common.py": HANDOUTS_DIR / "demo11_common.py",
    "demo11_app.py": HANDOUTS_DIR / "demo11_app.py",
    "demo11a_local_observable_roundtrip.py": (
        HANDOUTS_DIR / "demo11a_local_observable_roundtrip.py"
    ),
    "demo11b_confluent_observable_roundtrip.py": (
        HANDOUTS_DIR / "demo11b_confluent_observable_roundtrip.py"
    ),
    "tests/conftest.py": HANDOUTS_DIR / "demo11-tests" / "conftest.py",
    "tests/test_demo11_local.py": (
        HANDOUTS_DIR / "demo11-tests" / "test_demo11_local.py"
    ),
}

STUDENT_GITIGNORE = """# Credentials
.env
.env.*
!.env.example

# Environments and generated evidence
.venv/
venv/
outputs/
*.sqlite3

# Caches and local metadata
__pycache__/
*.py[cod]
.pytest_cache/
.DS_Store
"""

README_REPLACEMENTS = {
    "[Download `demo11-student.zip`](handouts/demo11-student.zip)": (
        "This extracted package already contains every Demo 11 student file."
    )
}


def _zip_info(name: str, *, is_dir: bool = False) -> ZipInfo:
    normalized = name.rstrip("/") + ("/" if is_dir else "")
    info = ZipInfo(normalized, ARCHIVE_TIMESTAMP)
    info.create_system = 3
    info.external_attr = ((0o755 if is_dir else 0o644) & 0xFFFF) << 16
    info.compress_type = ZIP_DEFLATED
    return info


def _validate_inputs() -> None:
    missing = [str(path) for path in SOURCE_MAP.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Missing Demo 11 source files: {missing}")
    unsafe = [
        name
        for name in SOURCE_MAP
        if PurePosixPath(name).is_absolute() or ".." in PurePosixPath(name).parts
    ]
    if unsafe:
        raise ValueError(f"Unsafe student ZIP paths: {unsafe}")
    env_rows = dict(
        line.split("=", 1)
        for line in SOURCE_MAP[".env.example"]
        .read_text(encoding="utf-8")
        .splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    )
    populated = [key for key in SECRET_ENV_KEYS if env_rows.get(key, "").strip()]
    if populated:
        raise ValueError(
            "Refusing to package populated credential fields: "
            + ", ".join(populated)
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
    _validate_inputs()
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

