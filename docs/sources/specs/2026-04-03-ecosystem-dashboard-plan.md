# Ecosystem Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a hybrid Sphinx + JS dashboard page to the zodb-pgjsonb docs showing all BlueDynamics ecosystem packages with live GitHub/PyPI data, cross-linked from all BD package docs via a Shibuya nav dropdown.

**Architecture:** A custom Sphinx extension renders static HTML cards (grouped by category) with data attributes. A JS script fetches live issue/PR counts and PyPI versions on page load, caching in sessionStorage (10 min TTL). CSS uses Shibuya theme variables for seamless integration.

**Tech Stack:** Sphinx extension (Python), vanilla JS (fetch API + sessionStorage), CSS custom properties (Shibuya theme vars)

**Spec:** `docs/sources/specs/2026-04-03-ecosystem-dashboard-design.md`

---

### Task 1: Sphinx Extension — `ecosystem_dashboard.py`

**Files:**
- Create: `sources/zodb-pgjsonb/docs/sources/_ext/ecosystem_dashboard.py`

This extension provides the `.. ecosystem-dashboard::` directive that parses YAML-like content and renders grouped HTML cards.

- [ ] **Step 1: Create the extension file**

```python
"""Sphinx extension: ecosystem-dashboard directive."""

from __future__ import annotations

import re
from docutils import nodes
from docutils.parsers.rst import Directive


def _parse_entries(content: list[str]) -> list[dict]:
    """Parse YAML-like entries from directive content.

    Each entry starts with ``- repo:`` and may contain
    ``pypi:``, ``docs:``, ``group:``, ``description:`` fields.
    """
    entries = []
    current = None
    for line in content:
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^-\s+(\w+):\s*(.*)$", line)
        if m and m.group(1) == "repo":
            current = {"repo": m.group(2).strip()}
            entries.append(current)
        elif m and current is not None:
            current[m.group(1)] = m.group(2).strip()
    return entries


def _render_card(entry: dict) -> str:
    """Render one package card as HTML."""
    repo = entry["repo"]
    name = repo.split("/", 1)[1]
    pypi = entry.get("pypi", "")
    docs = entry.get("docs", "")
    desc = entry.get("description", "")
    icon = entry.get("icon", "")

    gh_url = f"https://github.com/{repo}"

    links = []
    links.append(f'<a href="{gh_url}" target="_blank" rel="noopener">GitHub</a>')
    if pypi:
        links.append(
            f'<a href="https://pypi.org/project/{pypi}/" '
            f'target="_blank" rel="noopener">PyPI</a>'
        )
    if docs:
        links.append(
            f'<a href="{docs}" target="_blank" rel="noopener">Docs</a>'
        )
    links_html = " · ".join(links)

    pypi_version = ""
    if pypi:
        pypi_version = (
            f'<div class="eco-card-version">'
            f'Version: <span class="eco-pypi-version" data-pypi="{pypi}">...</span>'
            f"</div>"
        )

    icon_html = ""
    if icon:
        icon_html = (
            f'<img class="eco-card-icon" src="{icon}" alt="{name}" '
            f'loading="lazy" />'
        )

    return f"""<div class="eco-card" data-repo="{repo}">
  <div class="eco-card-top">
    {icon_html}
    <div class="eco-card-top-text">
      <div class="eco-card-header">
        <a href="{gh_url}" target="_blank" rel="noopener">{name}</a>
      </div>
      <div class="eco-card-desc">{desc}</div>
    </div>
  </div>
  {pypi_version}
  <div class="eco-card-stats">
    <span class="eco-issues" data-repo="{repo}">
      Issues: <span class="eco-count">...</span>
    </span>
    <span class="eco-separator">·</span>
    <span class="eco-prs" data-repo="{repo}">
      PRs: <span class="eco-count">...</span>
    </span>
  </div>
  <div class="eco-card-links">{links_html}</div>
</div>"""


class EcosystemDashboard(Directive):
    """Directive to render the ecosystem dashboard."""

    has_content = True

    def run(self):
        entries = _parse_entries(list(self.content))

        # Group by category
        groups: dict[str, list[dict]] = {}
        for entry in entries:
            group = entry.get("group", "Other")
            groups.setdefault(group, []).append(entry)

        # Preserve group order from input
        seen = []
        for entry in entries:
            g = entry.get("group", "Other")
            if g not in seen:
                seen.append(g)

        parts = []
        parts.append(
            '<div class="eco-dashboard">'
            '<div class="eco-header">'
            "<p>An overview of all packages in the BlueDynamics ZODB/Plone "
            "ecosystem — current releases and development activity.</p>"
            '<button class="eco-refresh" onclick="window.ecoDashboardRefresh()"'
            ' title="Refresh live data">↻ Refresh</button>'
            "</div>"
        )

        for group_name in seen:
            parts.append(f'<h3 class="eco-group-title">{group_name}</h3>')
            parts.append('<div class="eco-grid">')
            for entry in groups[group_name]:
                parts.append(_render_card(entry))
            parts.append("</div>")

        parts.append("</div>")

        raw = nodes.raw("", "\n".join(parts), format="html")
        return [raw]


def setup(app):
    app.add_directive("ecosystem-dashboard", EcosystemDashboard)
    app.add_css_file("dashboard.css")
    app.add_js_file("dashboard.js")
    return {"version": "1.0", "parallel_read_safe": True}
```

- [ ] **Step 2: Verify extension loads**

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/zodb-pgjsonb/docs
.venv/bin/sphinx-build -b html sources html 2>&1 | head -20
```

Expected: Build succeeds (extension loads but no page uses the directive yet).

- [ ] **Step 3: Register extension in conf.py**

In `sources/zodb-pgjsonb/docs/sources/conf.py`, add the `_ext` path and extension:

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "_ext"))
```

Add at the top of the file (before `project = ...`).

Then add `"ecosystem_dashboard"` to the `extensions` list:

```python
extensions = [
    "myst_parser",
    "sphinxcontrib.mermaid",
    "sphinx_design",
    "sphinx_copybutton",
    "ecosystem_dashboard",
]
```

- [ ] **Step 4: Commit**

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/zodb-pgjsonb
git add docs/sources/_ext/ecosystem_dashboard.py docs/sources/conf.py
git commit -m "feat(docs): add ecosystem-dashboard Sphinx extension"
```

---

### Task 2: Dashboard CSS — `dashboard.css`

**Files:**
- Create: `sources/zodb-pgjsonb/docs/sources/_static/dashboard.css`

- [ ] **Step 1: Create the CSS file**

```css
/* Ecosystem Dashboard — uses Shibuya theme CSS custom properties */

.eco-dashboard {
  margin-top: 1rem;
}

.eco-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 1.5rem;
  gap: 1rem;
}

.eco-header p {
  margin: 0;
  color: var(--color-text-secondary, #aaa);
  font-size: 0.95rem;
  flex: 1;
}

.eco-refresh {
  background: transparent;
  border: 1px solid var(--color-border, #444);
  color: var(--color-text, #eee);
  padding: 0.3rem 0.8rem;
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.85rem;
  white-space: nowrap;
  transition: border-color 0.2s, color 0.2s;
}

.eco-refresh:hover {
  border-color: var(--color-accent, cyan);
  color: var(--color-accent, cyan);
}

.eco-group-title {
  border-bottom: 1px solid var(--color-border, #444);
  padding-bottom: 0.4rem;
  margin-top: 2rem;
  margin-bottom: 1rem;
}

.eco-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 1rem;
  margin-bottom: 1rem;
}

.eco-card {
  background: var(--color-background-secondary, #1a1a2e);
  border: 1px solid var(--color-border, #444);
  border-radius: 8px;
  padding: 1.2rem;
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.eco-card-top {
  display: flex;
  gap: 0.8rem;
  align-items: flex-start;
}

.eco-card-icon {
  width: 40px;
  height: 40px;
  border-radius: 6px;
  flex-shrink: 0;
}

.eco-card-top-text {
  flex: 1;
  min-width: 0;
}

.eco-card-header a {
  font-size: 1.1rem;
  font-weight: 600;
  color: var(--color-text, #eee);
  text-decoration: none;
}

.eco-card-header a:hover {
  color: var(--color-accent, cyan);
}

.eco-card-desc {
  color: var(--color-text-secondary, #aaa);
  font-size: 0.9rem;
  line-height: 1.4;
}

.eco-card-version {
  font-size: 0.85rem;
  color: var(--color-text, #eee);
}

.eco-card-stats {
  font-size: 0.85rem;
  color: var(--color-text, #eee);
}

.eco-separator {
  margin: 0 0.3rem;
  color: var(--color-text-secondary, #aaa);
}

.eco-card-links {
  font-size: 0.85rem;
  margin-top: auto;
  padding-top: 0.5rem;
  border-top: 1px solid var(--color-border, #444);
}

.eco-card-links a {
  color: var(--color-accent, cyan);
  text-decoration: none;
}

.eco-card-links a:hover {
  text-decoration: underline;
}

.eco-issues a,
.eco-prs a {
  color: inherit;
  text-decoration: none;
}

.eco-issues a:hover,
.eco-prs a:hover {
  color: var(--color-accent, cyan);
}
```

- [ ] **Step 2: Commit**

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/zodb-pgjsonb
git add docs/sources/_static/dashboard.css
git commit -m "feat(docs): add ecosystem dashboard CSS using Shibuya theme vars"
```

---

### Task 3: Dashboard JS — `dashboard.js`

**Files:**
- Create: `sources/zodb-pgjsonb/docs/sources/_static/dashboard.js`

- [ ] **Step 1: Create the JS file**

```js
/**
 * Ecosystem Dashboard — live GitHub + PyPI data with sessionStorage cache.
 *
 * Fetches open issue/PR counts from GitHub API and latest version from PyPI.
 * Caches in sessionStorage for 10 minutes. Refresh button clears cache.
 */

(function () {
  "use strict";

  var CACHE_KEY = "ecosystem_dashboard_cache";
  var CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes

  function readCache() {
    try {
      var raw = sessionStorage.getItem(CACHE_KEY);
      if (!raw) return null;
      var cache = JSON.parse(raw);
      if (Date.now() - cache.timestamp > CACHE_TTL_MS) return null;
      return cache.data;
    } catch (e) {
      return null;
    }
  }

  function writeCache(data) {
    try {
      sessionStorage.setItem(
        CACHE_KEY,
        JSON.stringify({ timestamp: Date.now(), data: data })
      );
    } catch (e) {
      // sessionStorage full or unavailable — ignore
    }
  }

  function clearCache() {
    try {
      sessionStorage.removeItem(CACHE_KEY);
    } catch (e) {
      // ignore
    }
  }

  /**
   * Fetch GitHub open issues and PRs for a repo.
   * Issues endpoint includes PRs, so we subtract PR count.
   */
  async function fetchGitHub(repo) {
    var baseUrl = "https://api.github.com/repos/" + repo;

    var [repoResp, prsResp] = await Promise.all([
      fetch(baseUrl, { headers: { Accept: "application/vnd.github.v3+json" } }),
      fetch(baseUrl + "/pulls?state=open&per_page=100", {
        headers: { Accept: "application/vnd.github.v3+json" },
      }),
    ]);

    if (!repoResp.ok || !prsResp.ok) {
      return { issues: null, prs: null };
    }

    var repoData = await repoResp.json();
    var prsData = await prsResp.json();

    var totalOpen = repoData.open_issues_count || 0;
    var prCount = Array.isArray(prsData) ? prsData.length : 0;
    var issueCount = totalOpen - prCount;

    return { issues: issueCount, prs: prCount };
  }

  /** Fetch latest PyPI version. */
  async function fetchPyPI(pkg) {
    var resp = await fetch("https://pypi.org/pypi/" + pkg + "/json");
    if (!resp.ok) return null;
    var data = await resp.json();
    return data.info ? data.info.version : null;
  }

  /** Update DOM elements with fetched data. */
  function applyData(data) {
    // Update PyPI versions
    document.querySelectorAll(".eco-pypi-version").forEach(function (el) {
      var pypi = el.getAttribute("data-pypi");
      if (data[pypi] && data[pypi].version !== undefined) {
        el.textContent = data[pypi].version || "n/a";
      } else {
        el.textContent = "n/a";
      }
    });

    // Update issue counts
    document.querySelectorAll(".eco-issues").forEach(function (el) {
      var repo = el.getAttribute("data-repo");
      var countEl = el.querySelector(".eco-count");
      if (!countEl) return;
      if (data[repo] && data[repo].issues !== null) {
        var count = data[repo].issues;
        var link = document.createElement("a");
        link.href =
          "https://github.com/" +
          repo +
          "/issues?q=is%3Aissue+is%3Aopen";
        link.target = "_blank";
        link.rel = "noopener";
        link.textContent = count;
        countEl.textContent = "";
        countEl.appendChild(link);
      } else {
        countEl.textContent = "n/a";
      }
    });

    // Update PR counts
    document.querySelectorAll(".eco-prs").forEach(function (el) {
      var repo = el.getAttribute("data-repo");
      var countEl = el.querySelector(".eco-count");
      if (!countEl) return;
      if (data[repo] && data[repo].prs !== null) {
        var count = data[repo].prs;
        var link = document.createElement("a");
        link.href =
          "https://github.com/" + repo + "/pulls?q=is%3Apr+is%3Aopen";
        link.target = "_blank";
        link.rel = "noopener";
        link.textContent = count;
        countEl.textContent = "";
        countEl.appendChild(link);
      } else {
        countEl.textContent = "n/a";
      }
    });
  }

  /** Fetch all data for all repos/packages on the page. */
  async function fetchAll() {
    var data = {};
    var repos = new Set();
    var pypis = new Set();

    document.querySelectorAll(".eco-card").forEach(function (card) {
      var repo = card.getAttribute("data-repo");
      if (repo) repos.add(repo);
    });

    document.querySelectorAll(".eco-pypi-version").forEach(function (el) {
      var pypi = el.getAttribute("data-pypi");
      if (pypi) pypis.add(pypi);
    });

    var promises = [];

    repos.forEach(function (repo) {
      promises.push(
        fetchGitHub(repo).then(function (result) {
          data[repo] = Object.assign(data[repo] || {}, result);
        })
      );
    });

    pypis.forEach(function (pypi) {
      promises.push(
        fetchPyPI(pypi).then(function (version) {
          data[pypi] = Object.assign(data[pypi] || {}, { version: version });
        })
      );
    });

    await Promise.all(promises);
    return data;
  }

  /** Main: load from cache or fetch, then apply. */
  async function init() {
    // Only run if dashboard is on the page
    if (!document.querySelector(".eco-dashboard")) return;

    var cached = readCache();
    if (cached) {
      applyData(cached);
      return;
    }

    try {
      var data = await fetchAll();
      writeCache(data);
      applyData(data);
    } catch (e) {
      // Network error — leave "..." placeholders
      console.warn("Ecosystem dashboard: failed to fetch live data", e);
    }
  }

  /** Refresh: clear cache and re-fetch. */
  window.ecoDashboardRefresh = function () {
    clearCache();
    // Reset all counts to "..."
    document.querySelectorAll(".eco-count").forEach(function (el) {
      el.textContent = "...";
    });
    document.querySelectorAll(".eco-pypi-version").forEach(function (el) {
      el.textContent = "...";
    });
    init();
  };

  // Run on DOM ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
```

- [ ] **Step 2: Commit**

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/zodb-pgjsonb
git add docs/sources/_static/dashboard.js
git commit -m "feat(docs): add dashboard JS for live GitHub/PyPI data with caching"
```

---

### Task 4: Dashboard Page — `ecosystem.md`

**Files:**
- Create: `sources/zodb-pgjsonb/docs/sources/ecosystem.md`
- Modify: `sources/zodb-pgjsonb/docs/sources/index.md` (add to toctree)

- [ ] **Step 1: Create the ecosystem page**

````markdown
# Ecosystem

```{ecosystem-dashboard}

- repo: bluedynamics/zodb-pgjsonb
  pypi: zodb-pgjsonb
  docs: https://bluedynamics.github.io/zodb-pgjsonb/
  icon: https://bluedynamics.github.io/zodb-pgjsonb/_static/logo-web.png
  group: Storage
  description: PostgreSQL JSONB storage adapter for ZODB

- repo: bluedynamics/zodb-s3blobs
  pypi: zodb-s3blobs
  group: Storage
  description: S3 blob storage for ZODB

- repo: bluedynamics/plone-pgcatalog
  pypi: plone-pgcatalog
  docs: https://bluedynamics.github.io/plone-pgcatalog/
  icon: https://bluedynamics.github.io/plone-pgcatalog/_static/logo-web.png
  group: Catalog
  description: PostgreSQL-backed catalog for Plone

- repo: bluedynamics/plone-pgthumbor
  pypi: plone-pgthumbor
  docs: https://bluedynamics.github.io/plone-pgthumbor/
  icon: https://bluedynamics.github.io/plone-pgthumbor/_static/logo-web.png
  group: Image Processing
  description: Thumbor image scaling for Plone

- repo: bluedynamics/zodb-pgjsonb-thumborblobloader
  pypi: zodb-pgjsonb-thumborblobloader
  group: Image Processing
  description: Thumbor blob loader for zodb-pgjsonb

- repo: bluedynamics/zodb-json-codec
  pypi: zodb-json-codec
  docs: https://bluedynamics.github.io/zodb-json-codec/
  icon: https://bluedynamics.github.io/zodb-json-codec/_static/logo-web.png
  group: Tools & Libraries
  description: Rust-based pickle-to-JSON transcoder for ZODB

- repo: bluedynamics/zodb-convert
  pypi: zodb-convert
  group: Tools & Libraries
  description: Generic ZODB storage conversion tool

- repo: plone/cookiecutter-zope-instance
  docs: https://plone.github.io/cookiecutter-zope-instance/
  group: Tools & Libraries
  description: Cookiecutter template for Zope 5 WSGI instances

```
````

- [ ] **Step 2: Add to toctree in index.md**

In `sources/zodb-pgjsonb/docs/sources/index.md`, add `ecosystem` to the toctree. Change:

```markdown
```{toctree}
---
maxdepth: 3
caption: Documentation
titlesonly: true
hidden: true
---
tutorials/index
how-to/index
reference/index
explanation/index
```
```

To:

```markdown
```{toctree}
---
maxdepth: 3
caption: Documentation
titlesonly: true
hidden: true
---
ecosystem
tutorials/index
how-to/index
reference/index
explanation/index
```
```

- [ ] **Step 3: Build and verify**

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/zodb-pgjsonb/docs
.venv/bin/sphinx-build -b html sources html 2>&1 | tail -5
```

Expected: Build succeeds with no warnings about ecosystem-dashboard.

- [ ] **Step 4: Open in browser to visually verify**

```bash
xdg-open /home/jensens/ws/cdev/z3blobs/sources/zodb-pgjsonb/docs/html/ecosystem.html
```

Expected: Page shows grouped cards with "..." placeholders that fill in with live data.

- [ ] **Step 5: Commit**

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/zodb-pgjsonb
git add docs/sources/ecosystem.md docs/sources/index.md
git commit -m "feat(docs): add ecosystem dashboard page with all 8 packages"
```

---

### Task 5: Nav Dropdown — Update `conf.py` in All BlueDynamics Docs

**Files:**
- Modify: `sources/zodb-pgjsonb/docs/sources/conf.py:42-51`
- Modify: `sources/zodb-json-codec/docs/sources/conf.py:42-51`
- Modify: `sources/plone-pgcatalog/docs/sources/conf.py:42-51`
- Modify: `sources/plone-pgthumbor/docs/sources/conf.py:42-59`

The Ecosystem dropdown is the **first** entry in `nav_links`, followed by the existing package-specific links.

- [ ] **Step 1: Define the shared dropdown**

This is the Ecosystem dropdown entry that goes into every conf.py. Adapt the existing `nav_links` in each file to prepend this entry.

The dropdown definition (same for all 4 packages):

```python
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
            "summary": "PostgreSQL-backed catalog",
        },
        {
            "title": "plone-pgthumbor",
            "url": "https://bluedynamics.github.io/plone-pgthumbor/",
            "summary": "Thumbor image scaling",
        },
    ],
},
```

- [ ] **Step 2: Update zodb-pgjsonb conf.py**

Replace the `nav_links` value in `sources/zodb-pgjsonb/docs/sources/conf.py`:

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
                    "summary": "PostgreSQL-backed catalog",
                },
                {
                    "title": "plone-pgthumbor",
                    "url": "https://bluedynamics.github.io/plone-pgthumbor/",
                    "summary": "Thumbor image scaling",
                },
            ],
        },
        {
            "title": "GitHub",
            "url": "https://github.com/bluedynamics/zodb-pgjsonb",
        },
        {
            "title": "PyPI",
            "url": "https://pypi.org/project/zodb-pgjsonb/",
        },
    ],
```

- [ ] **Step 3: Update zodb-json-codec conf.py**

Replace the `nav_links` value in `sources/zodb-json-codec/docs/sources/conf.py`:

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
                    "summary": "PostgreSQL-backed catalog",
                },
                {
                    "title": "plone-pgthumbor",
                    "url": "https://bluedynamics.github.io/plone-pgthumbor/",
                    "summary": "Thumbor image scaling",
                },
            ],
        },
        {
            "title": "GitHub",
            "url": "https://github.com/bluedynamics/zodb-json-codec",
        },
        {
            "title": "PyPI",
            "url": "https://pypi.org/project/zodb-json-codec/",
        },
    ],
```

- [ ] **Step 4: Update plone-pgcatalog conf.py**

Replace the `nav_links` value in `sources/plone-pgcatalog/docs/sources/conf.py`:

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
                    "summary": "PostgreSQL-backed catalog",
                },
                {
                    "title": "plone-pgthumbor",
                    "url": "https://bluedynamics.github.io/plone-pgthumbor/",
                    "summary": "Thumbor image scaling",
                },
            ],
        },
        {
            "title": "GitHub",
            "url": "https://github.com/bluedynamics/plone-pgcatalog",
        },
        {
            "title": "PyPI",
            "url": "https://pypi.org/project/plone.pgcatalog/",
        },
    ],
```

- [ ] **Step 5: Update plone-pgthumbor conf.py**

Replace the `nav_links` value in `sources/plone-pgthumbor/docs/sources/conf.py`:

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
                    "summary": "PostgreSQL-backed catalog",
                },
                {
                    "title": "plone-pgthumbor",
                    "url": "https://bluedynamics.github.io/plone-pgthumbor/",
                    "summary": "Thumbor image scaling",
                },
            ],
        },
        {
            "title": "GitHub (addon)",
            "url": "https://github.com/bluedynamics/plone-pgthumbor",
        },
        {
            "title": "PyPI (addon)",
            "url": "https://pypi.org/project/plone.pgthumbor/",
        },
        {
            "title": "GitHub (loader)",
            "url": "https://github.com/bluedynamics/zodb-pgjsonb-thumborblobloader",
        },
        {
            "title": "PyPI (loader)",
            "url": "https://pypi.org/project/zodb-pgjsonb-thumborblobloader/",
        },
    ],
```

- [ ] **Step 6: Build zodb-pgjsonb docs to verify dropdown renders**

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/zodb-pgjsonb/docs
.venv/bin/sphinx-build -b html sources html 2>&1 | tail -5
```

Expected: Build succeeds. Open `html/index.html` in browser — "Ecosystem" dropdown visible in header with 5 children (Dashboard first).

- [ ] **Step 7: Commit all conf.py changes**

```bash
cd /home/jensens/ws/cdev/z3blobs
git add sources/zodb-pgjsonb/docs/sources/conf.py \
      sources/zodb-json-codec/docs/sources/conf.py \
      sources/plone-pgcatalog/docs/sources/conf.py \
      sources/plone-pgthumbor/docs/sources/conf.py
git commit -m "feat(docs): add Ecosystem nav dropdown to all BlueDynamics package docs"
```

---

### Task 6: Update GitHub Actions Workflow

**Files:**
- Modify: `sources/zodb-pgjsonb/.github/workflows/docs.yaml:8`

The workflow currently only triggers on `docs/**` changes. The new `_ext/` directory is inside `docs/sources/`, so it's already covered. However, the `sys.path` insert in `conf.py` expects the extension to be at build time — no additional dependencies needed since the extension is pure Python using only `docutils` (already a Sphinx dependency).

No changes needed to the workflow — the extension uses only built-in Sphinx/docutils APIs.

- [ ] **Step 1: Verify by running a clean build**

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/zodb-pgjsonb/docs
rm -rf html
.venv/bin/sphinx-build -b html sources html
```

Expected: Clean build succeeds, `html/ecosystem.html` exists with cards and JS/CSS loaded.

- [ ] **Step 2: Open final page in browser**

```bash
xdg-open /home/jensens/ws/cdev/z3blobs/sources/zodb-pgjsonb/docs/html/ecosystem.html
```

Expected: Dashboard page with 4 groups, 8 cards, live data loading. Refresh button works. Cards styled consistently with Shibuya theme.
