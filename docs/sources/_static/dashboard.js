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
