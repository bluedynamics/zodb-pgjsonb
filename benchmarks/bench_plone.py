"""Plone benchmark worker — runs in a subprocess per backend.

Called by bench.py plone subcommand. Not intended for direct use.

Configures Zope2, creates a Plone site, benchmarks real Plone operations,
outputs JSON results to stdout.

Usage (internal):
    python bench_plone.py --conf /path/to/zope.conf --backend BackendName
"""

from __future__ import annotations

import argparse
import io
import json
import os
import statistics
import sys
import time

import transaction


def setup_zope(conf_path):
    """Configure Zope2 and return the application."""
    # Suppress Zope startup noise (ZCML loading, logger setup, etc.)
    saved_stdout = sys.stdout
    saved_stderr = sys.stderr
    sys.stdout = io.StringIO()
    sys.stderr = open(os.devnull, "w")
    try:
        from Zope2.Startup.run import configure_wsgi

        configure_wsgi(conf_path)
    finally:
        sys.stderr.close()
        sys.stderr = saved_stderr
        sys.stdout = saved_stdout

    import Zope2
    from Testing.makerequest import makerequest

    app = Zope2.app()
    return makerequest(app)


def setup_admin(app):
    """Create admin user and set up security context."""
    from AccessControl.SecurityManagement import newSecurityManager

    uf = app.acl_users
    if not uf.getUserById("admin"):
        uf.userFolderAddUser("admin", "admin", ["Manager"], [])
    transaction.commit()

    admin = uf.getUserById("admin")
    newSecurityManager(None, admin.__of__(uf))


def bench_site_creation(app):
    """Create a Plone site. Returns (elapsed_ms, site)."""
    from plone.distribution.api.site import create
    from Products.CMFPlone.factory import _DEFAULT_PROFILE

    config = {
        "site_id": "bench_site",
        "title": "Benchmark Site",
        "setup_content": False,
        "default_language": "en",
        "portal_timezone": "UTC",
        "extension_ids": ["plone.volto:default"],
        "profile_id": _DEFAULT_PROFILE,
    }

    t0 = time.perf_counter()
    site = create(app, "volto", config)
    transaction.commit()
    t1 = time.perf_counter()
    return (t1 - t0) * 1000.0, site


def bench_content_creation(site, n=50):
    """Create N documents individually with commit per doc."""
    from zope.component.hooks import setSite

    setSite(site)

    samples = []
    for i in range(n):
        t0 = time.perf_counter()
        site.invokeFactory("Document", f"doc-{i}", title=f"Benchmark Document {i}")
        transaction.commit()
        t1 = time.perf_counter()
        samples.append((t1 - t0) * 1000.0)
    return samples


def bench_catalog_query(site, n=50):
    """Run N catalog queries for Documents."""
    catalog = site.portal_catalog

    samples = []
    for _ in range(n):
        t0 = time.perf_counter()
        results = catalog.searchResults(portal_type="Document")
        _count = len(results)  # force evaluation
        t1 = time.perf_counter()
        samples.append((t1 - t0) * 1000.0)
    return samples


def bench_content_modification(site, n=50):
    """Modify N existing documents (title change + reindex)."""
    samples = []
    for i in range(n):
        doc = site[f"doc-{i}"]
        t0 = time.perf_counter()
        doc.title = f"Modified Document {i}"
        doc.reindexObject()
        transaction.commit()
        t1 = time.perf_counter()
        samples.append((t1 - t0) * 1000.0)
    return samples


def _stats_dict(samples):
    """Convert timing samples to a stats dictionary."""
    if not samples:
        return {}
    return {
        "count": len(samples),
        "mean_ms": round(statistics.mean(samples), 3),
        "median_ms": round(statistics.median(samples), 3),
        "min_ms": round(min(samples), 3),
        "max_ms": round(max(samples), 3),
        "total_ms": round(sum(samples), 3),
    }


def main():
    parser = argparse.ArgumentParser(description="Plone benchmark worker")
    parser.add_argument("--conf", required=True, help="Path to zope.conf")
    parser.add_argument("--backend", required=True, help="Backend name")
    parser.add_argument("--docs", type=int, default=50, help="Number of documents")
    args = parser.parse_args()

    app = setup_zope(args.conf)
    setup_admin(app)

    results = {"backend": args.backend}
    n_docs = args.docs

    # Benchmark: site creation
    site_time, site = bench_site_creation(app)
    results["site_creation_ms"] = round(site_time, 3)

    # Benchmark: content creation
    create_samples = bench_content_creation(site, n_docs)
    results["content_creation"] = _stats_dict(create_samples)

    # Benchmark: catalog query
    query_samples = bench_catalog_query(site, n_docs)
    results["catalog_query"] = _stats_dict(query_samples)

    # Benchmark: content modification
    modify_samples = bench_content_modification(site, n_docs)
    results["content_modification"] = _stats_dict(modify_samples)

    # Output JSON on stdout (only output in the whole process)
    print(json.dumps(results))


if __name__ == "__main__":
    main()
