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
   * Uses search API: one query for issues, one for PRs — same cost but
   * returns total_count directly without needing to fetch all items.
   */
  async function fetchGitHub(repo) {
    var headers = { Accept: "application/vnd.github.v3+json" };
    var searchBase = "https://api.github.com/search/issues?q=repo:" + repo;

    var [issuesResp, prsResp] = await Promise.all([
      fetch(searchBase + "+type:issue+state:open", { headers: headers }),
      fetch(searchBase + "+type:pr+state:open", { headers: headers }),
    ]);

    if (issuesResp.status === 403 || prsResp.status === 403) {
      var reset = issuesResp.headers.get("x-ratelimit-reset") ||
                  prsResp.headers.get("x-ratelimit-reset");
      if (reset) {
        var mins = Math.ceil((parseInt(reset) * 1000 - Date.now()) / 60000);
        console.warn("GitHub rate limit hit. Resets in " + mins + " min.");
      }
      return { issues: null, prs: null };
    }

    if (!issuesResp.ok || !prsResp.ok) {
      return { issues: null, prs: null };
    }

    var issuesData = await issuesResp.json();
    var prsData = await prsResp.json();

    return {
      issues: issuesData.total_count || 0,
      prs: prsData.total_count || 0,
    };
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
