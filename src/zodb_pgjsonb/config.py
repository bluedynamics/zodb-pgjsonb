"""ZConfig factory for PGJsonbStorage."""

from ZODB.config import BaseConfig


class PGJsonbStorageFactory(BaseConfig):
    """ZConfig factory — called when Zope parses a <pgjsonb> section."""

    def open(self, database_name="unnamed", databases=None):
        from .storage import PGJsonbStorage

        config = self.config

        # Build S3 client + blob cache if bucket is configured.
        # Imports are lazy so zodb_s3blobs is only required when S3
        # is actually configured (optional dependency via [s3] extra).
        s3_client = None
        blob_cache = None
        if getattr(config, "s3_bucket_name", None):
            try:
                from zodb_s3blobs.s3client import S3Client
                from zodb_s3blobs.cache import S3BlobCache
            except ImportError as exc:
                raise ImportError(
                    "S3 blob storage requires the [s3] extra: "
                    "pip install zodb-pgjsonb[s3]"
                ) from exc

            s3_client = S3Client(
                bucket_name=config.s3_bucket_name,
                prefix=getattr(config, "s3_prefix", "") or "",
                endpoint_url=getattr(config, "s3_endpoint_url", None),
                region_name=getattr(config, "s3_region", None),
                aws_access_key_id=getattr(config, "s3_access_key", None),
                aws_secret_access_key=getattr(config, "s3_secret_key", None),
                use_ssl=getattr(config, "s3_use_ssl", True),
            )
            cache_dir = (
                getattr(config, "blob_cache_dir", None)
                or config.blob_temp_dir
            )
            cache_size = getattr(config, "blob_cache_size", 1024 * 1024 * 1024)
            blob_cache = S3BlobCache(
                cache_dir=cache_dir,
                max_size=cache_size,
            )

        return PGJsonbStorage(
            dsn=config.dsn,
            name=config.name,
            history_preserving=config.history_preserving,
            blob_temp_dir=config.blob_temp_dir,
            cache_local_mb=config.cache_local_mb,
            pool_size=config.pool_size,
            pool_max_size=config.pool_max_size,
            s3_client=s3_client,
            blob_cache=blob_cache,
            blob_threshold=getattr(config, "blob_threshold", 1024 * 1024),
        )
