"""Sphinx extension: ecosystem-dashboard directive."""

from __future__ import annotations

from docutils import nodes
from docutils.parsers.rst import Directive

import re


def _parse_entries(content: list[str]) -> list[dict]:
    """Parse YAML-like entries from directive content.

    Each entry starts with ``- repo:`` and may contain
    ``pypi:``, ``docs:``, ``icon:``, ``group:``, ``description:`` fields.
    """
    entries = []
    current = None
    for line in content:
        stripped = line.strip()
        if not stripped:
            continue
        # New entry: starts with "- repo:"
        m = re.match(r"^-\s+(\w+):\s*(.*)$", stripped)
        if m and m.group(1) == "repo":
            current = {"repo": m.group(2).strip()}
            entries.append(current)
            continue
        # Continuation field (indented, no leading dash)
        m = re.match(r"^(\w+):\s*(.*)$", stripped)
        if m and current is not None:
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
    title_url = docs if docs else gh_url

    links = []
    links.append(f'<a href="{gh_url}" target="_blank" rel="noopener">GitHub</a>')
    if pypi:
        links.append(
            f'<a href="https://pypi.org/project/{pypi}/" '
            f'target="_blank" rel="noopener">PyPI</a>'
        )
    if docs:
        links.append(f'<a href="{docs}" target="_blank" rel="noopener">Docs</a>')
    links_html = " &middot; ".join(links)

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
            f'<img class="eco-card-icon" src="{icon}" alt="{name}" loading="lazy" />'
        )

    return f"""<div class="eco-card" data-repo="{repo}">
  <div class="eco-card-top">
    {icon_html}
    <div class="eco-card-top-text">
      <div class="eco-card-header">
        <a href="{title_url}" target="_blank" rel="noopener">{name}</a>
      </div>
      <div class="eco-card-desc">{desc}</div>
    </div>
  </div>
  {pypi_version}
  <div class="eco-card-stats">
    <span class="eco-issues" data-repo="{repo}">
      Issues: <span class="eco-count">...</span>
    </span>
    <span class="eco-separator">&middot;</span>
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
            "<p>An overview of all packages in the BlueDynamics cloud-native PostgreSQL ZODB/Plone "
            "ecosystem \u2014 current releases and development activity.</p>"
            '<button class="eco-refresh" onclick="window.ecoDashboardRefresh()"'
            ' title="Refresh live data">\u21bb Refresh</button>'
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
