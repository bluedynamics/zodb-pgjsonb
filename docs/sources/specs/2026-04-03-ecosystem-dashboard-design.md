# Ecosystem Dashboard — Design Spec

**Date:** 2026-04-03
**Location:** zodb-pgjsonb docs (`docs/sources/ecosystem.md`)

## Goal

A single dashboard page showing all BlueDynamics ZODB/Plone packages at a glance:
per-package links (GitHub, PyPI, Docs), current PyPI version, and live counts of
open issues and PRs. Hosted as part of the zodb-pgjsonb Sphinx documentation,
cross-linked from all BlueDynamics package docs via a Shibuya nav dropdown.

## Packages (8 total, 4 groups)

### Storage
| Package | GitHub | PyPI | Docs |
|---------|--------|------|------|
| zodb-pgjsonb | bluedynamics/zodb-pgjsonb | zodb-pgjsonb | bluedynamics.github.io/zodb-pgjsonb/ |
| zodb-s3blobs | bluedynamics/zodb-s3blobs | zodb-s3blobs | — |

### Catalog
| Package | GitHub | PyPI | Docs |
|---------|--------|------|------|
| plone-pgcatalog | bluedynamics/plone-pgcatalog | plone-pgcatalog | bluedynamics.github.io/plone-pgcatalog/ |

### Image Processing
| Package | GitHub | PyPI | Docs |
|---------|--------|------|------|
| plone-pgthumbor | bluedynamics/plone-pgthumbor | plone-pgthumbor | bluedynamics.github.io/plone-pgthumbor/ |
| zodb-pgjsonb-thumborblobloader | bluedynamics/zodb-pgjsonb-thumborblobloader | zodb-pgjsonb-thumborblobloader | — |

### Tools & Libraries
| Package | GitHub | PyPI | Docs |
|---------|--------|------|------|
| zodb-json-codec | bluedynamics/zodb-json-codec | zodb-json-codec | bluedynamics.github.io/zodb-json-codec/ |
| zodb-convert | bluedynamics/zodb-convert | zodb-convert | — |
| cookiecutter-zope-instance | plone/cookiecutter-zope-instance | — (template) | plone.github.io/cookiecutter-zope-instance/ |

## Architecture: Hybrid Sphinx + Client-Side JS

### Build-Time (Sphinx Extension)

A custom Sphinx extension `_ext/ecosystem_dashboard.py` provides a directive
`.. ecosystem-dashboard::` that renders static HTML:

- Grouped cards layout (4 groups as above)
- Each card contains: package name, short description, link placeholders
  (GitHub, PyPI, Docs)
- Placeholder `<span>` elements with `data-repo` and `data-pypi` attributes
  for JS to fill in live data (version, issue count, PR count)
- A refresh button in the top-right corner

The directive reads package definitions from its content body (YAML-like):

```rst
.. ecosystem-dashboard::

   - repo: bluedynamics/zodb-pgjsonb
     pypi: zodb-pgjsonb
     docs: https://bluedynamics.github.io/zodb-pgjsonb/
     group: Storage
     description: PostgreSQL JSONB storage adapter for ZODB

   - repo: bluedynamics/zodb-s3blobs
     pypi: zodb-s3blobs
     group: Storage
     description: S3 blob storage for ZODB

   - repo: bluedynamics/plone-pgcatalog
     pypi: plone-pgcatalog
     docs: https://bluedynamics.github.io/plone-pgcatalog/
     group: Catalog
     description: PostgreSQL-backed catalog for Plone

   - repo: bluedynamics/plone-pgthumbor
     pypi: plone-pgthumbor
     docs: https://bluedynamics.github.io/plone-pgthumbor/
     group: Image Processing
     description: Thumbor image scaling for Plone

   - repo: bluedynamics/zodb-pgjsonb-thumborblobloader
     pypi: zodb-pgjsonb-thumborblobloader
     group: Image Processing
     description: Thumbor blob loader for zodb-pgjsonb

   - repo: bluedynamics/zodb-json-codec
     pypi: zodb-json-codec
     docs: https://bluedynamics.github.io/zodb-json-codec/
     group: Tools & Libraries
     description: Rust-based pickle↔JSON transcoder for ZODB

   - repo: bluedynamics/zodb-convert
     pypi: zodb-convert
     group: Tools & Libraries
     description: Generic ZODB storage conversion tool

   - repo: plone/cookiecutter-zope-instance
     docs: https://plone.github.io/cookiecutter-zope-instance/
     group: Tools & Libraries
     description: Cookiecutter template for Zope 5 WSGI instances
```

### Client-Side JS (`_static/dashboard.js`)

Fetches live data on page load and fills in the placeholder elements.

**GitHub API** — for each repo:
- `GET https://api.github.com/repos/{owner}/{repo}` → open_issues_count
  (includes PRs, so also fetch PRs separately)
- `GET https://api.github.com/repos/{owner}/{repo}/pulls?state=open&per_page=1`
  → use the response `Link` header or array length with `per_page=1` +
  parse total from Link header, or simply fetch all open PRs (few expected)
- Issues count = `open_issues_count` minus open PRs count

**PyPI API** — for each package:
- `GET https://pypi.org/pypi/{package}/json` → `info.version`

**Caching strategy:**
- Store results in `sessionStorage` under key `ecosystem_dashboard_cache`
- Value: JSON object `{ timestamp: <epoch_ms>, data: { "<repo>": { issues, prs, version } } }`
- TTL: 10 minutes — if cache exists and is younger than TTL, use cached data
- Refresh button: clears cache and re-fetches everything

**UI update flow:**
1. On page load, check `sessionStorage` for valid cache
2. If cache hit: populate immediately from cache
3. If cache miss or expired: show "..." placeholders, fetch all APIs in parallel
4. On success: update DOM elements, write to cache
5. On error (rate limit, network): show "n/a" for affected fields

**Rate limit budget:**
- 8 repos × 2 requests (repo info + PRs) = 16 GitHub requests
- 7 PyPI packages × 1 request = 7 PyPI requests
- Total: 23 requests per load
- GitHub unauthenticated limit: 60/hour/IP → ~2-3 full refreshes per hour
- With 10-minute sessionStorage cache, practical usage is well within limits

### CSS (`_static/dashboard.css`)

All styling uses Shibuya CSS custom properties for seamless theme integration:

```
Cards:
  background:    var(--color-background-secondary)
  border:        1px solid var(--color-border)
  border-radius: var(--border-radius) or 8px
  padding:       1.2rem

Group headings:
  font/color:    inherit from theme h3
  border-bottom: 1px solid var(--color-border)

Grid:
  display: grid
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr))
  gap: 1rem

Links (GitHub/PyPI/Docs badges):
  color:      var(--color-accent)
  font-size:  0.85rem

Live data (version, issues, PRs):
  Inline text, normal theme text color
  "..." while loading
  "n/a" on error

Refresh button:
  Top-right of dashboard container
  Subtle styling, accent color on hover
```

No custom colors — everything derives from Shibuya's theme variables, so it works
in both dark and light mode (if ever switched).

## Navigation: Shibuya Dropdown

All BlueDynamics package docs get an "Ecosystem" dropdown as the **first**
`nav_links` entry. The dropdown contains:

1. **Dashboard** (links to the ecosystem page) — first entry
2. Individual package docs (direct links)

Example `conf.py` entry (same structure for all 4 BlueDynamics docs):

```python
"nav_links": [
    {
        "title": "Ecosystem",
        "url": "https://bluedynamics.github.io/zodb-pgjsonb/ecosystem.html",
        "children": [
            {
                "title": "Dashboard",
                "url": "https://bluedynamics.github.io/zodb-pgjsonb/ecosystem.html",
                "summary": "Overview of all packages",
            },
            {
                "title": "zodb-pgjsonb",
                "url": "https://bluedynamics.github.io/zodb-pgjsonb/",
                "summary": "PostgreSQL JSONB storage",
            },
            {
                "title": "zodb-json-codec",
                "url": "https://bluedynamics.github.io/zodb-json-codec/",
                "summary": "Rust pickle↔JSON transcoder",
            },
            {
                "title": "plone-pgcatalog",
                "url": "https://bluedynamics.github.io/plone-pgcatalog/",
                "summary": "PostgreSQL-backed catalog for Plone",
            },
            {
                "title": "plone-pgthumbor",
                "url": "https://bluedynamics.github.io/plone-pgthumbor/",
                "summary": "Thumbor image scaling for Plone",
            },
        ],
    },
    # ... existing GitHub, PyPI links
]
```

**Affected conf.py files** (4 packages):
- `sources/zodb-pgjsonb/docs/sources/conf.py`
- `sources/zodb-json-codec/docs/sources/conf.py`
- `sources/plone-pgcatalog/docs/sources/conf.py`
- `sources/plone-pgthumbor/docs/sources/conf.py`

**NOT affected:**
- `sources/cookiecutter-zope-instance/docs/sources/conf.py` — broader scope (Plone org),
  listed in dashboard but does not link back

## File Structure

All new files live in the zodb-pgjsonb docs tree:

```
sources/zodb-pgjsonb/docs/sources/
├── _ext/
│   └── ecosystem_dashboard.py    # Sphinx extension (directive)
├── _static/
│   ├── dashboard.js              # Client-side API fetching + caching
│   └── dashboard.css             # Card/grid styling with Shibuya vars
├── ecosystem.md                  # Page using the directive
└── specs/
    └── 2026-04-03-ecosystem-dashboard-design.md  # This file
```

The `ecosystem.md` page needs to be added to the docs `toctree` (in `index.md`).

## Page Structure

The `ecosystem.md` page starts with a short intro (max 2 lines) explaining what
the dashboard shows, e.g.:

> **BlueDynamics ZODB/Plone Ecosystem** — an overview of all packages, their
> current releases, and development activity.

Followed by the `.. ecosystem-dashboard::` directive which renders the cards.

## Card Layout (per package)

```
┌──────────────────────────────────────────┐
│ [icon]  {package-name}                    │
│         {description}                     │
│                                           │
│ Version: {pypi-version}                   │
│ Issues: {count}  ·  PRs: {count}          │
│                                           │
│ [GitHub]  [PyPI]  [Docs]                  │
└───────────────────────────────────────────┘
```

- Icon: optional, 40x40px project logo (from GitHub Pages `_static/logo-web.png`),
  configured via `icon:` field in directive content. Packages without icons show no image.
- Package name: bold, links to GitHub repo
- Description: one line, muted text
- Version: fetched live from PyPI (or "n/a")
- Issues/PRs: fetched live from GitHub (or "n/a"), each links to
  the filtered GitHub issues/pulls page
- Footer links: small badges/text links, only shown if URL exists
  (e.g. no PyPI link for cookiecutter-zope-instance, no Docs for some packages)

## Out of Scope

- Authentication for GitHub API (unauthenticated is sufficient)
- CI/build status badges
- Changelog or release history display
- Package dependency graph
- Automated updates to the package list (manual YAML in the directive)
