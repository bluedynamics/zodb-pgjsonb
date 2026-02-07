"""ZConfig factory for PGJsonbStorage."""

from ZODB.config import BaseConfig


class PGJsonbStorageFactory(BaseConfig):
    """ZConfig factory — called when Zope parses a <pgjsonb> section."""

    def open(self, database_name="unnamed", databases=None):
        from .storage import PGJsonbStorage

        config = self.config
        return PGJsonbStorage(
            dsn=config.dsn,
            name=config.name,
            history_preserving=config.history_preserving,
            blob_temp_dir=config.blob_temp_dir,
        )
