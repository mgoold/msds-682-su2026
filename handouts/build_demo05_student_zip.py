"""Build Demo 05 student ZIP directly from published handout sources."""

from __future__ import annotations

import argparse
from pathlib import Path, PurePosixPath
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

HANDOUTS_DIR = Path(__file__).resolve().parent
SITE_ROOT = HANDOUTS_DIR.parent
OUTPUT_PATH = HANDOUTS_DIR / "demo05-student.zip"
PACKAGE_ROOT = "demo05-student"
ARCHIVE_TIMESTAMP = (2026, 7, 20, 0, 0, 0)
PACKAGE_DIRECTORIES = ("tests", "assets", "assets/demo05")
SCREENSHOT_NAMES = (
    "demo05a-expected-result.jpg",
    "demo05b-swagger-overview.jpg",
    "demo05b-local-202-response.jpg",
    "demo05c-expected-result.jpg",
    "demo05c-confluent-topic.jpg",
    "demo05c-schema-registry.jpg",
    "demo05d-cloud-202-response.jpg",
)
SECRET_ENV_KEYS = (
    "BOOTSTRAP_SERVERS",
    "SASL_USERNAME",
    "SASL_PASSWORD",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
)

SOURCE_MAP: dict[str, Path] = {
    "README.md": HANDOUTS_DIR / "demo05.md",
    "fastapi-recap.md": HANDOUTS_DIR / "fastapi-recap.md",
    "requirements.txt": HANDOUTS_DIR / "requirements.txt",
    ".env.example": HANDOUTS_DIR / ".env.example",
    "confluent_demo_common.py": HANDOUTS_DIR / "confluent_demo_common.py",
    "trip_event_contract.py": HANDOUTS_DIR / "trip_event_contract.py",
    "trip_event_v1.avsc": HANDOUTS_DIR / "trip_event_v1.avsc",
    "trip_event_v2_reader.avsc": HANDOUTS_DIR / "trip_event_v2_reader.avsc",
    "demo05_common.py": HANDOUTS_DIR / "demo05_common.py",
    "demo05_app.py": HANDOUTS_DIR / "demo05_app.py",
    "demo05_kafka.py": HANDOUTS_DIR / "demo05_kafka.py",
    "demo05a_fastapi_contract.py": HANDOUTS_DIR / "demo05a_fastapi_contract.py",
    "demo05b_fastapi_local_service.py": HANDOUTS_DIR
    / "demo05b_fastapi_local_service.py",
    "demo05c_confluent_fastapi_roundtrip.py": HANDOUTS_DIR
    / "demo05c_confluent_fastapi_roundtrip.py",
    "demo05d_live_confluent_service.py": HANDOUTS_DIR
    / "demo05d_live_confluent_service.py",
    "tests/conftest.py": HANDOUTS_DIR / "demo05-tests" / "conftest.py",
    "tests/test_demo05_local.py": HANDOUTS_DIR
    / "demo05-tests"
    / "test_demo05_local.py",
    **{
        f"assets/demo05/{name}": SITE_ROOT / "assets" / "demo05" / name
        for name in SCREENSHOT_NAMES
    },
}

STUDENT_GITIGNORE = """# Credentials
.env
.env.*
!.env.example

# Environments and generated evidence
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
    "(#/handouts/fastapi-recap)": "(fastapi-recap.md)",
    "- [Download `demo05-student.zip`](handouts/demo05-student.zip)": (
        "- This extracted package already contains all Demo 05 student files."
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
        raise FileNotFoundError(f"Missing Demo 05 source files: {missing}")
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
    """Read one SSOT source and adapt website-only README links for the ZIP."""

    if archive_name != "README.md":
        return source_path.read_bytes()
    content = source_path.read_text(encoding="utf-8")
    for website_text, package_text in README_REPLACEMENTS.items():
        if website_text not in content:
            raise ValueError(f"Missing expected README text: {website_text}")
        content = content.replace(website_text, package_text)
    return content.encode("utf-8")


def build_student_zip(output_path: Path = OUTPUT_PATH) -> Path:
    """Create one deterministic ZIP without a second source tree."""

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
