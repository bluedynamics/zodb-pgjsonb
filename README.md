# zodb-pgjsonb

ZODB storage adapter for PostgreSQL using JSONB, powered by [zodb-json-codec](https://github.com/bluedynamics/zodb-json-codec).

`zodb-pgjsonb` stores object state as **queryable JSONB** instead of opaque pickle bytea.
ZODB sees pickle bytes at its boundaries; PostgreSQL sees queryable JSON internally.

- **SQL Queryability** -- query ZODB objects directly with PostgreSQL JSONB operators and GIN indexes
- **Performance** -- Rust-based codec, psycopg3 pipelined writes, pure SQL pack/GC (15-28x faster than RelStorage)
- **Tiered Blobs** -- small blobs in PG bytea, large blobs in S3 (configurable threshold)
- **Full ZODB Compatibility** -- IStorage, IMVCCStorage, IBlobStorage, IStorageUndoable, IStorageIteration, IStorageRestoreable
- **Security** -- JSON has no code execution attack surface (unlike pickle deserialization)

Requires Python 3.12+, PostgreSQL 14+.

## Quick Start

```xml
%import zodb_pgjsonb

<zodb_db main>
    <pgjsonb>
        dsn dbname=zodb user=zodb password=zodb host=localhost port=5432
    </pgjsonb>
</zodb_db>
```

```bash
pip install zodb-pgjsonb          # or: pip install zodb-pgjsonb[s3]
```

## Documentation

Full documentation: **https://bluedynamics.github.io/zodb-pgjsonb/**

- [Quickstart (Docker)](https://bluedynamics.github.io/zodb-pgjsonb/tutorials/quickstart-docker.html) -- Plone + PostgreSQL + MinIO in 5 minutes
- [Migration guide](https://bluedynamics.github.io/zodb-pgjsonb/tutorials/migrate-from-filestorage.html) -- migrate from FileStorage or RelStorage
- [Configuration reference](https://bluedynamics.github.io/zodb-pgjsonb/reference/configuration.html) -- all ZConfig and Python API options
- [Architecture](https://bluedynamics.github.io/zodb-pgjsonb/explanation/architecture.html) -- design decisions and data flow
- [Performance benchmarks](https://bluedynamics.github.io/zodb-pgjsonb/explanation/performance.html) -- detailed comparison with RelStorage
- [SQL query examples](https://bluedynamics.github.io/zodb-pgjsonb/how-to/query-with-sql.html) -- querying ZODB data with SQL
- [State processor plugins](https://bluedynamics.github.io/zodb-pgjsonb/how-to/write-state-processor.html) -- writing extra columns alongside object state

## Source Code and Contributions

The source code is managed in a Git repository, with its main branches hosted on GitHub.
Issues can be reported there too.

We'd be happy to see many forks and pull requests to make this package even better.
We welcome AI-assisted contributions, but expect every contributor to fully understand and be able to explain the code they submit.
Please don't send bulk auto-generated pull requests.

Maintainers are Jens Klein and the BlueDynamics Alliance developer team.
We appreciate any contribution and if a release on PyPI is needed, please just contact one of us.
We also offer commercial support if any training, coaching, integration or adaptations are needed.

## License

ZPL-2.1 (Zope Public License)
