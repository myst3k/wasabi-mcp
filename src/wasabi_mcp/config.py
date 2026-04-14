from __future__ import annotations

import os
import platform
from dataclasses import dataclass, field
from pathlib import Path

WASABI_BASE_ENDPOINT = "https://s3.wasabisys.com"
WASABI_IAM_ENDPOINT = "https://iam.wasabisys.com"

# Fallback region map in case ?describeRegions call fails.
# Format: region_name -> S3 endpoint URL
FALLBACK_REGION_MAP: dict[str, str] = {
    "us-east-1": "https://s3.us-east-1.wasabisys.com",
    "us-east-2": "https://s3.us-east-2.wasabisys.com",
    "us-west-1": "https://s3.us-west-1.wasabisys.com",
    "us-central-1": "https://s3.us-central-1.wasabisys.com",
    "eu-central-1": "https://s3.eu-central-1.wasabisys.com",
    "eu-central-2": "https://s3.eu-central-2.wasabisys.com",
    "eu-west-1": "https://s3.eu-west-1.wasabisys.com",
    "eu-west-2": "https://s3.eu-west-2.wasabisys.com",
    "ap-northeast-1": "https://s3.ap-northeast-1.wasabisys.com",
    "ap-northeast-2": "https://s3.ap-northeast-2.wasabisys.com",
    "ap-southeast-1": "https://s3.ap-southeast-1.wasabisys.com",
    "ap-southeast-2": "https://s3.ap-southeast-2.wasabisys.com",
    "ca-central-1": "https://s3.ca-central-1.wasabisys.com",
}


def _default_index_db_path() -> Path:
    system = platform.system()
    if system == "Darwin":
        base = Path.home() / "Library" / "Application Support"
    elif system == "Windows":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    else:
        base = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return base / "wasabi-mcp" / "index.db"


@dataclass
class WasabiConfig:
    access_key_id: str | None = None
    secret_access_key: str | None = None
    aws_profile: str | None = None
    region: str = "us-east-1"
    iam_endpoint: str = WASABI_IAM_ENDPOINT
    index_db_path: Path = field(default_factory=_default_index_db_path)
    region_map: dict[str, str] = field(default_factory=lambda: dict(FALLBACK_REGION_MAP))

    @classmethod
    def from_env(cls) -> WasabiConfig:
        access_key = os.environ.get("WASABI_ACCESS_KEY_ID")
        secret_key = os.environ.get("WASABI_SECRET_ACCESS_KEY")
        profile = os.environ.get("AWS_PROFILE") if not access_key else None
        region = os.environ.get("WASABI_REGION", "us-east-1")
        iam_endpoint = os.environ.get("WASABI_IAM_ENDPOINT", WASABI_IAM_ENDPOINT)
        db_path_str = os.environ.get("WASABI_INDEX_DB_PATH")
        db_path = Path(db_path_str) if db_path_str else _default_index_db_path()

        return cls(
            access_key_id=access_key,
            secret_access_key=secret_key,
            aws_profile=profile,
            region=region,
            iam_endpoint=iam_endpoint,
            index_db_path=db_path,
        )

    def endpoint_for_region(self, region: str) -> str:
        return self.region_map.get(region, f"https://s3.{region}.wasabisys.com")
