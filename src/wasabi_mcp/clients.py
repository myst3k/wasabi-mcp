from __future__ import annotations

import asyncio
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Any

import boto3
import botocore.auth
import botocore.awsrequest
import botocore.credentials
from botocore.config import Config as BotoConfig

from wasabi_mcp.config import WASABI_BASE_ENDPOINT, WasabiConfig

logger = logging.getLogger(__name__)

# S3 namespace in Wasabi's XML responses
S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _make_session(config: WasabiConfig) -> boto3.Session:
    if config.access_key_id and config.secret_access_key:
        return boto3.Session(
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
            region_name=config.region,
        )
    return boto3.Session(
        profile_name=config.aws_profile,
        region_name=config.region,
    )


def _make_s3_client(session: boto3.Session, endpoint_url: str, region: str) -> Any:
    return session.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


@dataclass
class BucketInfo:
    name: str
    creation_date: str
    region: str
    public_access: str = "Unknown"
    versioning: str = "Unknown"


@dataclass
class ClientManager:
    config: WasabiConfig
    _session: boto3.Session = field(init=False, repr=False)
    _base_client: Any = field(init=False, repr=False)
    _iam_client: Any = field(init=False, repr=False)
    _region_clients: dict[str, Any] = field(default_factory=dict, init=False)
    _bucket_regions: dict[str, str] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self._session = _make_session(self.config)
        self._base_client = _make_s3_client(
            self._session, WASABI_BASE_ENDPOINT, self.config.region
        )
        self._iam_client = self._session.client(
            "iam",
            endpoint_url=self.config.iam_endpoint,
            region_name=self.config.region,
        )

    @property
    def iam(self) -> Any:
        return self._iam_client

    def s3_for_region(self, region: str) -> Any:
        if region not in self._region_clients:
            endpoint = self.config.endpoint_for_region(region)
            self._region_clients[region] = _make_s3_client(
                self._session, endpoint, region
            )
        return self._region_clients[region]

    async def s3_for_bucket(self, bucket: str) -> Any:
        region = self._bucket_regions.get(bucket)
        if not region:
            region = await self._resolve_bucket_region(bucket)
            self._bucket_regions[bucket] = region
        return self.s3_for_region(region)

    def get_cached_bucket_region(self, bucket: str) -> str | None:
        return self._bucket_regions.get(bucket)

    async def _resolve_bucket_region(self, bucket: str) -> str:
        try:
            resp = await asyncio.to_thread(
                self._base_client.get_bucket_location, Bucket=bucket
            )
            location = resp.get("LocationConstraint") or self.config.region
            return location
        except Exception:
            logger.warning(f"Could not resolve region for bucket {bucket}, using default")
            return self.config.region

    async def list_buckets_extended(self) -> list[BucketInfo]:
        """List buckets using Wasabi's ?publicAccess&location extension.

        Falls back to standard ListBuckets + per-bucket GetBucketLocation
        if the extended call fails.
        """
        try:
            return await self._list_buckets_wasabi_extended()
        except Exception:
            logger.info("Extended ListBuckets failed, falling back to standard API")
            return await self._list_buckets_standard()

    async def _list_buckets_wasabi_extended(self) -> list[BucketInfo]:
        """Make a signed GET to the regional endpoint with ?publicAccess&location."""
        import urllib.request

        endpoint = self.config.endpoint_for_region(self.config.region)
        url = f"{endpoint}/?publicAccess&location"

        credentials = self._session.get_credentials()
        if credentials is None:
            raise RuntimeError("No AWS credentials available")
        credentials = credentials.get_frozen_credentials()

        signer = botocore.auth.S3SigV4Auth(
            botocore.credentials.Credentials(
                credentials.access_key,
                credentials.secret_key,
                credentials.token,
            ),
            "s3",
            self.config.region,
        )

        request = botocore.awsrequest.AWSRequest(method="GET", url=url)
        signer.add_auth(request)

        signed_url = request.url
        headers = dict(request.headers)

        def _fetch() -> bytes:
            req = urllib.request.Request(signed_url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read()

        body = await asyncio.to_thread(_fetch)
        return self._parse_extended_list_buckets(body)

    def _parse_extended_list_buckets(self, xml_bytes: bytes) -> list[BucketInfo]:
        root = ET.fromstring(xml_bytes)
        buckets_el = root.find(f"{{{S3_NS}}}Buckets")
        if buckets_el is None:
            # Try without namespace
            buckets_el = root.find("Buckets")
        if buckets_el is None:
            return []

        result: list[BucketInfo] = []
        for bucket_el in buckets_el:
            name = self._xml_text(bucket_el, "Name", "")
            creation_date = self._xml_text(bucket_el, "CreationDate", "")
            region = (
                self._xml_text(bucket_el, "BucketRegion", "")
                or self._xml_text(bucket_el, "Region", "")
                or self.config.region
            )
            public_access = self._xml_text(bucket_el, "PublicAccess", "Unknown")
            versioning = self._xml_text(bucket_el, "Versioning", "Unknown")

            if name:
                self._bucket_regions[name] = region
                result.append(BucketInfo(
                    name=name,
                    creation_date=creation_date,
                    region=region,
                    public_access=public_access,
                    versioning=versioning,
                ))
        return result

    def _xml_text(self, el: ET.Element, tag: str, default: str) -> str:
        child = el.find(f"{{{S3_NS}}}{tag}")
        if child is None:
            child = el.find(tag)
        return child.text if child is not None and child.text else default

    async def _list_buckets_standard(self) -> list[BucketInfo]:
        resp = await asyncio.to_thread(self._base_client.list_buckets)
        result: list[BucketInfo] = []
        for b in resp.get("Buckets", []):
            name = b["Name"]
            creation_date = b["CreationDate"].isoformat() if hasattr(b["CreationDate"], "isoformat") else str(b["CreationDate"])
            region = await self._resolve_bucket_region(name)
            self._bucket_regions[name] = region
            result.append(BucketInfo(
                name=name,
                creation_date=creation_date,
                region=region,
            ))
        return result

    async def fetch_region_map(self) -> None:
        """Fetch the dynamic region map from ?describeRegions."""
        try:
            url = f"{WASABI_BASE_ENDPOINT}/?describeRegions"

            def _fetch() -> bytes:
                import urllib.request

                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return resp.read()

            body = await asyncio.to_thread(_fetch)
            root = ET.fromstring(body)

            for region_el in root.iter():
                # Look for region elements and extract name + endpoint
                name_el = region_el.find("RegionName")
                endpoint_el = region_el.find("RegionEndpoint")
                if name_el is not None and endpoint_el is not None:
                    name = name_el.text
                    endpoint = endpoint_el.text
                    if name and endpoint:
                        if not endpoint.startswith("https://"):
                            endpoint = f"https://{endpoint}"
                        self.config.region_map[name] = endpoint
            logger.info(f"Loaded {len(self.config.region_map)} regions from describeRegions")
        except Exception as e:
            logger.warning(f"Failed to fetch describeRegions, using fallback map: {e}")

    def close(self) -> None:
        for client in self._region_clients.values():
            try:
                client.close()
            except Exception:
                pass
        try:
            self._base_client.close()
        except Exception:
            pass
        try:
            self._iam_client.close()
        except Exception:
            pass
