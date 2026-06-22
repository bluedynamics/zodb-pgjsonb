# Release Process

## Version Management

This project uses `hatch-vcs` — the git tag is the single source of truth for
the version. There is no version string in any file to maintain manually.

## Prerequisites

### PyPI Trusted Publishing (one-time setup)

Both Test PyPI and PyPI use OIDC trusted publishing (no API tokens needed).

1. **Test PyPI**: https://test.pypi.org/manage/project/zodb-pgjsonb/settings/publishing/
   - Add a GitHub publisher: owner=`bluedynamics`, repo=`zodb-pgjsonb`,
     workflow=`release.yaml`, environment=`release-test-pypi`

2. **PyPI**: https://pypi.org/manage/project/zodb-pgjsonb/settings/publishing/
   - Add a GitHub publisher: owner=`bluedynamics`, repo=`zodb-pgjsonb`,
     workflow=`release.yaml`, environment=`release-pypi`

3. **GitHub Environments**: In the repo settings, create two environments:
   - `release-test-pypi`
   - `release-pypi` (optionally add required reviewers for extra safety)

### Tools

```bash
uv tool install hatch  # optional, for local builds
```

## Making a Release

### 1. Finalize the changelog

Every change lands in `CHANGES.md` under the open `## unreleased` section.
To release, promote that heading to the new version:

```markdown
## 1.13.1
```

Keep the entries grouped (`### Bugfixes`, `### Features`, `### Tests`, …) and
order most-important first. Commit this via a short release PR (direct pushes
to `main` are blocked by branch protection):

```bash
git checkout -b release/1.13.1
git commit -am "docs: finalize CHANGES for 1.13.1 release"
git push -u origin release/1.13.1
gh pr create --base main --title "Release 1.13.1" --fill
```

### 2. Merge once CI is green, then update `main`

```bash
gh pr merge <PR#> --squash --delete-branch
git checkout main && git pull --ff-only origin main
```

### 3. Tag the release

Tags follow PEP 440. The `v` prefix is conventional here (`v1.13.0`, `v1.13.1`):

```bash
git tag -a v1.13.1 -m "Release 1.13.1"
```

For pre-releases:

```bash
git tag v1.14.0a1   # alpha
git tag v1.14.0b1   # beta
git tag v1.14.0rc1  # release candidate
```

### 4. Push the tag

```bash
git push origin v1.13.1
```

### 5. Create a GitHub Release

The **published** GitHub release is what triggers the PyPI upload
(`release-pypi` job in `release.yaml`).

```bash
# release notes = the section you just finalized in CHANGES.md
notes=$(awk '/^## 1\.13\.1/{f=1;next} /^## /{f=0} f' CHANGES.md)
gh release create v1.13.1 --title "1.13.1" --notes "$notes"
```

Or via the UI: https://github.com/bluedynamics/zodb-pgjsonb/releases/new —
select the tag, set the title, paste the changelog section, "Publish release".
For pre-releases, mark it as a pre-release.

### 6. Verify

```bash
gh run watch --exit-status   # watch the release.yaml run finish
```

- Check https://pypi.org/project/zodb-pgjsonb/ for the new version
- Verify installation: `uv pip install zodb-pgjsonb==1.13.1`

## Dev Builds to Test PyPI

On every successful **CI** run on `main`, `release.yaml` also builds the
package and publishes an in-dev version to **Test PyPI** (`release-test-pypi`
job). No action needed — this gives a continuously installable dev build
between releases.

## Building Locally (for testing)

```bash
hatch build           # creates sdist + wheel in dist/
ls dist/
# zodb_pgjsonb-1.13.1.tar.gz
# zodb_pgjsonb-1.13.1-py3-none-any.whl
```

Without a git tag on the current commit, `hatch-vcs` generates a dev version
like `1.13.1.dev3+g0123abc` (`local_scheme = "no-local-version"` strips the
`+g…` suffix for uploaded artifacts). This is expected for development builds.

## Checklist

- [ ] All tests pass (`pytest`, requires PostgreSQL on `localhost:5433` —
      see README / CLAUDE.md)
- [ ] `CHANGES.md` `## unreleased` section promoted to the new version
- [ ] `main` branch is clean and CI is green
- [ ] Tag follows PEP 440 with a `v` prefix (`v1.13.1`)
- [ ] GitHub Release published with the changelog section as notes
- [ ] Package visible on https://pypi.org/project/zodb-pgjsonb/
