# Plan: bring docs/sources to the plone-doc-style 0.5.0 rules

Spec: `/home/jensens/.claude/plugins/cache/plone-doc-style-skill/plone-doc-style/0.5.0/skills/plone-doc-style/SKILL.md` (the style guide) and the review report `/home/jensens/ws/cdev/cloudbrine/plans/2026-11-10-zodb-pgjsonb-docstyle-review.md` (findings with line numbers, written against commit 33e0af7).
Line numbers in this plan refer to that commit. Earlier tasks shift lines; search for the quoted text.

## Global constraints

These bind every task.

- **Scope.** Only files under `docs/sources/` except `docs/sources/specs/`, plus the files a task names explicitly. Never touch `docs/plans/`, `docs/superpowers/`, `src/`, `tests/`, `README.md`. `CHANGES.md` only in Task 13.
- **Facts are sacred.** Preserve every technical fact. Add no fact you have not verified in `src/zodb_pgjsonb/`. When a sentence is ambiguous, keep its meaning and ask in your report rather than guess.
- **Code blocks.** Never change the content of a code block except where a task says so. Lexer names may change per the lexer table.
- **One sentence per line.** Every sentence on its own line. Never break a sentence across lines. Never two sentences on one line. Table rows, list items and directive options are exempt from wrapping rules but a list item still holds whole sentences.
- **Lexer table.** `shell` for command lines (never `bash`, never a `$` or `#` prompt), `console` for a session that shows a prompt and output, `python`, `sql` for plain SQL, `postgresql` for SQL that contains `$$` or PL/pgSQL, `psql` for blocks with backslash meta-commands such as `\q`, `ini` for ZConfig fragments (`%import`, `<pgjsonb>`) and for `postgresql.conf`, `toml`, `text` for everything else (crontab lines, log output). No bare fences.
- **Labels.** Every H2 heading gets a `(label)=` line, a blank line, then the heading. Name: `<file-basename>-<heading-slug>` with the heading lowercased, non-alphanumerics to single hyphens, trimmed. Example: in `how-to/install.md`, `## Set up PostgreSQL` gets `(install-set-up-postgresql)=`. The six existing labels (`cache-tiers`, `shared-cache-consensus-gate`, `startup-ddl-gate`, `blob-downloads-request-path`, `gil-convoy`, `size-threads-gil`) keep their names. An H3 gets a label only when something links to it.
- **Cross-references.** Link with `` {ref}`label` `` or `` {doc}`/path` ``; never a Markdown link to a `.md` file or a `#anchor`.
- **Tool naming.** The migration tool is `zodb-convert` (package `zodb-convert`, command `zodb-convert`). The word `zodbconvert` must not appear in the docs after Task 1. The filename `how-to/migrate-with-zodbconvert.md` stays (published URL).
- **Attribution.** Every commit message ends, after a blank line, with `Assisted-by: <your model name>`, for example `Assisted-by: Claude Sonnet 5.5`. Never `Co-Authored-By`, never an email address.
- **Commits.** One commit per task, message in English, subject `docs: <what>`. Do not commit `.superpowers/`, `docs/html/`, or any venv.
- **Verification commands.**
  - Prose: `bash /home/jensens/.claude/plugins/cache/plone-doc-style-skill/plone-doc-style/0.5.0/skills/plone-doc-style/scripts/check-prose.sh <files>` (prints `clean` or a line list; exit 1 when it finds something).
  - Build: `/tmp/claude-1000/-home-jensens-ws-cdev-cloudbrine/14be1fd5-9e80-4b6c-917f-e7f7486e7156/scratchpad/docs-venv/bin/sphinx-build -E -a -q --keep-going docs/sources /tmp/claude-1000/-home-jensens-ws-cdev-cloudbrine/14be1fd5-9e80-4b6c-917f-e7f7486e7156/scratchpad/html-task` run from the worktree root. Baseline is 18 warnings (file `.../scratchpad/sphinx-baseline-warnings.txt`). A task must not add a warning; tasks that fix lexers or labels remove some.
  - Grep: `grep -rn 'zodbconvert' docs/sources` must be empty after Task 1.
- **Working directory.** `/home/jensens/ws/cdev/cloudbrine/sources/zodb-pgjsonb-wt/docs/plone-doc-style`. Never the shared checkout `sources/zodb-pgjsonb/`.

### Task 1: Fix stale facts

Model: standard. Files: `docs/sources/reference/configuration.md`, `docs/sources/reference/storage-api.md`, `docs/sources/reference/schema.md`, `docs/sources/reference/state-processor-api.md`, `docs/sources/how-to/deploy-production.md`, `docs/sources/how-to/write-state-processor.md`, `docs/sources/how-to/migrate-with-zodbconvert.md`, `docs/sources/explanation/why-postgresql-jsonb.md`, `docs/sources/tutorials/migrate-from-filestorage.md`.

Verify each fact below against the code before writing it. Make only these changes; style fixes come in later tasks.

1. **Constructor signature** (`configuration.md:77-90` block and `:93-104` table, `storage-api.md:31-43` block). Source of truth `src/zodb_pgjsonb/storage.py:431-455`:
   `dsn`, `name="pgjsonb"`, `history_preserving=False`, `blob_temp_dir=None`, `cache_local_mb=None` (deprecated alias for `cache_shared_mb`, emits `DeprecationWarning`), `cache_shared_mb=256`, `cache_per_connection_mb=16`, `pool_size=1`, `pool_max_size=10`, `pool_timeout=30.0`, `s3_client=None`, `blob_cache=None`, `blob_cache_size=1024 * 1024 * 1024`, `blob_threshold=102_400`, `cache_warm_pct=10`, `cache_warm_decay=0.8`, `cache_warm_delay=15`, `cache_warm_jitter=30`, `cache_warm_concurrency=2`, `cache_warm_wait_max=300`, `cache_warm_batch_size=500`, `cache_warm_batch_pause=0.5`.
   Keep the annotated-signature style of the existing block; infer the type from the default (`int`, `float`, `str | None`, and so on). Add a table row per new parameter; take the description wording from the matching ZConfig key in the table above it (`cache-shared-mb`, `cache-per-connection-mb`, `blob-cache-size`, `cache-warm-*`). Exception: `cache_warm_pct` (and the existing ZConfig row `cache-warm-pct`) must read "Percentage of the cache's estimated object capacity to pre-warm at startup; the estimate derives from `cache-per-connection-mb` and the average object size (`storage.py:618-623`). Set to `0` to disable." because the code sizes the warm target from `cache_per_connection_mb`, not from `cache_shared_mb`. Check `component.xml` if a ZConfig key has no row.
2. **Deprecated cache key in the production guide** (`deploy-production.md:49-62`, section "Tune the object cache"). The section recommends `cache-local-mb`, which `configuration.md:37` marks deprecated since 1.12.0, and says the default is 16 MB. Rewrite the section around `cache-shared-mb` (process-wide L2, default 256 MB) and `cache-per-connection-mb` (per-connection L1, default 16 MB); keep the ZConfig example; keep "Set to 0 to disable"; end with `` {ref}`cache-tiers` `` for the model.
3. **How state-processor DDL is applied.** The docs say the DDL is tried at registration on an autocommit connection with a 2-second `lock_timeout` and deferred only when blocked. The code (`storage.py:864-881` `_apply_processor_ddl`, `storage.py:1040-1095` `_apply_stale_entries`, `startup_locks.py`) always defers: registration only queues the DDL, version-tagged with `sha256` of its text; `_apply_pending_ddl()` runs the queue at the next `poll_invalidations()` or `tpc_begin()` after the read snapshot has ended, and again on later calls while entries remain queued (`instance.py:281-290`, `instance.py:613`); it takes a session-level advisory lock whose acquisition uses `lock_timeout` = `ZODB_PGJSONB_DDL_LOCK_TIMEOUT` (default `15min`); each queue entry (one processor's whole `get_schema_sql()` text) then runs on its own direct autocommit connection with `lock_timeout = '30s'`; the schema-version gate runs before the lock is taken and is checked again inside it; an entry that fails with `psycopg.OperationalError` is requeued, any other error drops it until the next startup; the schema-version gate skips entries whose version marker is current.
   Correct these places to say that, tersely and as facts (these are reference pages): `schema.md:150-154`; `state-processor-api.md:38-45` (the `get_schema_sql` entry) and `:165-181` ("DDL application"); `write-state-processor.md:85-89`; `storage-api.md:333-346` (keep the example; fix the prose). Do not add rationale; later tasks link to the explanation.
4. **Unknown ecosystem name** (`why-postgresql-jsonb.md:122`): "the z3blobs ecosystem" becomes "the PostgreSQL-native ecosystem around zodb-pgjsonb".
5. **zodbconvert does not exist.** `tutorials/migrate-from-filestorage.md:27` claims ZODB ships `zodbconvert`; it does not, and zodb-pgjsonb installs nothing by that name (`pyproject.toml` dependencies). The ecosystem tool is `zodb-convert` (`/home/jensens/ws/cdev/cloudbrine/sources/zodb-convert`, CLI flags: `--workers/-w`, `--incremental`, `--background-blobs`, `--deferred-blobs`, `--upload-blobs`, `--source-zope-conf`, `--dest-zope-conf`, `--source-db`, `--dest-db`, `--dry-run`, `--verbose`; there is no `--clear`).
   - Step 1 (lines 18-27): install both: `uv pip install zodb-pgjsonb zodb-convert`; replace line 27 with "This also installs the `zodb-convert` command."
   - Step 5 (lines 85-102): command becomes `zodb-convert migrate.cfg`; delete the `--clear` sentence (line 93); the database from Step 2 is empty. Replace the invented output block at lines 97-102 and its lead-in with one sentence: "zodb-convert logs progress while it copies and prints a summary when it finishes." Leave lines 109-161 (parallel section) for Task 5, except: replace `zodbconvert` with `zodb-convert` wherever it still appears in the file (line 112, 240, 241).
   - `how-to/migrate-with-zodbconvert.md`: title (line 3) becomes "How to migrate with zodb-convert"; line 128 `zodbconvert` becomes `zodb-convert`.
   - `reference/storage-api.md:180`: "and `zodbconvert`" becomes "and `zodb-convert`".
   - Finish with `grep -rn zodbconvert docs/sources`; only the filename may match.

Verification: the grep above; `sphinx-build` warning count unchanged (18); read your diff once against the code paths cited.

### Task 2: Mechanical markup, reference pages

Model: standard. Files: `docs/sources/reference/storage-api.md`, `docs/sources/reference/state-processor-api.md`, `docs/sources/reference/schema.md`, `docs/sources/reference/configuration.md`, `docs/sources/reference/index.md`.

Do not rewrite sentences; only the operations below.

1. **Rejoin wrapped sentences.** The reference pages are hard-wrapped at about 72 columns (`storage-api.md` has about 54 continuation lines, `state-processor-api.md` 25, `schema.md` 7, `configuration.md` 3). Join every sentence onto one line. Definition-list bodies (lines starting with `: ` and their indented continuation) hold one sentence per line too, each continuation line indented by two spaces.
2. **Labels** on every H2 per the naming rule. Also add `(jsonb-state-structure)=` above `## JSONB state structure` in `schema.md` and change the link at `schema.md:34` from `[JSONB state structure](#jsonb-state-structure)` to `` {ref}`jsonb-state-structure` `` (this removes one Sphinx warning).
3. **Lexers.** `schema.md:108` trigger block: `postgresql`. `configuration.md:14` ZConfig block already `ini`; keep. Every `bash` fence becomes `shell`. Any bare fence gets a lexer from the table.
4. **Version directives.** `storage-api.md:296` `*Added in v1.9.2.*`, `:331` `*Added in v1.10.3.*`, `:419` `*Added in v1.8.0.*`: delete the italic line and add a `{versionadded} 1.9.2` (and so on) directive as the last element of that definition body, indented like the body. `storage-api.md:341-343` "Since v1.19.2 that DSN carries a `connect_timeout` ...": make the sentence present tense and add a `{versionchanged} 1.19.2` directive stating that the DSN carries `connect_timeout`. `configuration.md:37` row: shorten the cell to "Deprecated alias for `cache-shared-mb`; emits a `DeprecationWarning` when set." and add after the table a `{deprecated} 1.12.0` directive: "`cache-local-mb`. Use `cache-shared-mb` instead."
5. **List items with dash labels.** `storage-api.md:189-195` parameter list uses ` — `; change each item to the form `` - `other`: source storage ... ``. Nested `blob_mode` values the same way.
6. **Headings.** None to change in this group.

Verification: `check-prose.sh` on the five files must report no `two-sentences`, no `h1-count`, no `code-ellipsis`; other rules may remain (Task 12 handles them). Sphinx warning count drops by at least 2 (schema xref, schema sql lexing).

### Task 3: Mechanical markup, tutorials and how-to guides

Model: standard. Files: everything under `docs/sources/tutorials/` and `docs/sources/how-to/`.

Do not rewrite sentences; only the operations below.

1. **Rejoin wrapped sentences**: `migrate-with-zodbconvert.md` (lines 97-103, 135-137, 140-141, 147-148, 163-167), `deploy-production.md:23-27`, `install.md:44-46`.
2. **Labels** on every H2 per the naming rule, including the three H2s in `how-to/index.md`. Keep `size-threads-gil` and `blob-downloads-request-path` as they are.
3. **Lexers.** All `bash` fences become `shell`. ZConfig fragments tagged `xml` become `ini` (`install.md:53`, `configure-s3-blobs.md:21, 107, 159`, `switch-history-modes.md:18, 65`, `deploy-production.md:11, 53`, `quickstart-docker.md:91`, `migrate-from-filestorage.md:62, 167`). `migrate-from-filestorage.md:38` block with `\q` becomes `psql`. `deploy-production.md:105` crontab becomes `text`; `:135` and `:145` `postgresql.conf` fragments become `ini`.
4. **Alternative-command blocks.** `install.md:9-13` and `configure-s3-blobs.md:9-13` show `pip` and `uv pip` separated by `# or`. Keep one block with the `pip install ...` line only, followed by the sentence "With uv, run `uv pip install <same package spec>`." `migrate-with-zodbconvert.md:155-161`: split into two blocks; the `# Step 1:` / `# Step 2:` comments become one sentence of prose before each block.
5. **UI elements** in `quickstart-docker.md:132-134, 142-151, 229`: `**Add new**` style labels become `` {guilabel}`Add new` ``; "click **State: Private** and select **Publish**" becomes `` {menuselection}`State: Private --> Publish` ``; credentials such as `admin / admin` go in inline code, not bold.
6. **Admonitions.** `migrate-from-filestorage.md`: fold the tip at line 80-83 into the preceding paragraph as plain sentences; delete the note at 104-107; keep the note at 186-189 and the tip at 158-161 (Task 5 removes that section). `query-with-sql.md:142-146`: delete the H2 "Important: read-only" and put its three sentences in a `{warning}` directly after the intro sentence (line 5). `migrate-with-zodbconvert.md:166-167`: the bold `**Note:**` paragraph becomes a `{note}` whose second sentence reads "The staging directory needs enough disk space for them." `install.md:43-47`: delete the note (it restates line 21).
7. **Headings.** `configure-s3-blobs.md:68, 72, 77`: the bold run-in labels `**PG-only mode (no S3):**`, `**Tiered mode (default with S3):**`, `**S3-only mode:**` become H3 headings "PG-only mode", "Tiered mode", "S3-only mode"; the sentence after each colon stays as the first line of the section.

Verification: `check-prose.sh` over the two directories reports no `two-sentences`, no `shell-prompt`; sphinx warning count drops by at least 4 (xml lexing in install, configure-s3-blobs, quickstart, migrate-from-filestorage; sql lexing of `\q`).

### Task 4: Mechanical markup, explanation pages and landing page

Model: standard. Files: everything under `docs/sources/explanation/`, `docs/sources/index.md`, `docs/sources/tutorials/index.md`, `docs/sources/reference/index.md`, `docs/sources/explanation/index.md` (only labels/wrapping where applicable).

Do not rewrite sentences; only the operations below.

1. **Rejoin wrapped sentences**: `performance.md:270-271, 277-280`.
2. **Labels** on every H2 per the naming rule. Keep the four existing labels. In `performance.md` also label these H3s, which later tasks link to: `### Pack and garbage collection` is an H2 (label `performance-pack-and-garbage-collection`); H3 `### Pluggable refs prefetch (1.9.0--1.9.2)` gets `(performance-refs-prefetch)=`; H3 `### Batch object loading (1.8.0)` gets `(performance-batch-object-loading)=`.
3. **Lexers.** `architecture.md:303` trigger block: `postgresql`. `why-postgresql-jsonb.md:34, 48` plain SQL stays `sql`. All `bash` fences become `shell`.
4. **Mermaid labels** `architecture.md:62-63`: `ANY(...)` becomes `ANY(zoids)`; `INSERT...ON CONFLICT` becomes `INSERT ON CONFLICT`. `why-postgresql-jsonb.md:37` SQL comment: `-- state: \x80049513000000000000008c0b...  (unreadable binary)` becomes `-- state: binary pickle bytes, unreadable`.
5. **Headings.** `performance.md` H3s "HP writes", "HP reads", "HP undo and pack", "HP optimization impact" become "History-preserving writes", "History-preserving reads", "History-preserving undo and pack", "History-preserving optimization impact". `index.md:41` card title "How-To Guides" becomes "How-to guides".
6. **List items with dash labels.** `performance.md:273-275` (`- `_l2_load_hits` -- ...`) become colon form. `index.md:20-27` capability bullets: replace ` -- ` with a colon or a comma so each bullet is one sentence without a dash. `index.md:34-62` card bodies: one sentence each, for example "Step-by-step lessons for readers new to zodb-pgjsonb."

Verification: `check-prose.sh` over the files reports no `two-sentences`, no `code-ellipsis`; sphinx warning count drops by 1 (architecture sql lexing).

### Task 5: Diataxis cleanup, tutorials

Model: most capable. Files: `docs/sources/tutorials/quickstart-docker.md`, `docs/sources/tutorials/migrate-from-filestorage.md`.

Read the Tutorials section of `/home/jensens/.claude/plugins/cache/plone-doc-style-skill/plone-doc-style/0.5.0/skills/plone-doc-style/references/diataxis.md` first. A tutorial is one guaranteed path with no choices and no explanation; it ends with its last step or a `{seealso}`.

1. **Endings.** Delete `## What we learned` and `## Next steps` in both files. Replace with a `{seealso}` admonition holding the two links that were under Next steps, one per line, each a sentence.
2. **Choices.** Prerequisites "uv (recommended) or pip" becomes "[uv](https://docs.astral.sh/uv/)" in both files. `quickstart-docker.md` "Step 9: start pgAdmin (optional)": delete the section and renumber the following step. `migrate-from-filestorage.md` "Parallel migration for large databases" (the H3 and everything to the tip that ends it): delete; add one sentence at the end of Step 5: "For large databases, {doc}`/how-to/migrate-with-zodbconvert` shows how to run the copy with several workers."
3. **Honest expected output.** Keep only output blocks that the commands produce verbatim-shaped (`docker compose ps`, waitress log line, psql tables). If a block was invented, replace it with a sentence saying what the reader should see.
4. **Narrative.** Keep "we will", "Notice that", "You should see". Each step still ends with something visible. Do not add explanation; where a sentence explains a mechanism (for example `quickstart-docker.md:114-115` on blob thresholds), keep it only if the reader needs it to proceed; otherwise link to the explanation.
5. **Verify** both tutorials still read as a single path from Step 1 to the last step after the deletions (step numbers consecutive, no reference to a removed section).

Verification: `check-prose.sh` on both files reports no `stock-conclusion`; sphinx warning count unchanged.

### Task 6: Diataxis cleanup, how-to guides

Model: most capable. Files: everything under `docs/sources/how-to/` except `index.md`.

Read the How-to section of `references/diataxis.md` (path in Task 5). A how-to gives action only, links to reference and explanation, and names goals in its headings.

1. `switch-history-modes.md:7-12`: replace the two-mode explanation with one sentence and `` {ref}`architecture-history-modes` `` (label created in Task 4).
2. `migrate-with-zodbconvert.md` "Resume an interrupted migration" (lines 97-103): keep the command and one sentence ("Already committed transactions are skipped."); move nothing, delete the watermark mechanics (they remain in `storage-api.md` under `copyTransactionsFrom`).
3. `query-with-sql.md`: delete the `object_state` column table (section "The object_state table") and the JSONB operator table (section "JSONB operators" intro table only, keep the three example subsections as H2s); replace each with one sentence linking to `` {ref}`schema-tables` `` and to the PostgreSQL JSONB functions page `https://www.postgresql.org/docs/current/functions-json.html`.
4. `deploy-production.md` "Roll out without a DDL stampede" (lines 64-82): keep the first sentence, then one action-only lead-in ("The storage serializes this DDL behind an advisory lock and skips it when the schema is already current, with no configuration needed." and "To tune it, use two environment variables:"), the two bullets with the environment variables, and the `{seealso}`; delete the rest of the explanatory paragraphs. Rename headings: "PostgreSQL tuning" to "Tune PostgreSQL", "Backup strategy" to "Back up the database and blobs", "Monitoring queries" to "Monitor the database"; rename their labels to match.
5. `write-state-processor.md` "Security considerations" (lines 132-142): replace the section with a `{warning}` of two sentences ("Only register processors from trusted, audited code." and "A processor's SQL expressions are interpolated unescaped; see {ref}`state-processor-api-security`.") placed directly after the intro sentence at line 5; delete the H2.
6. Every guide opens with "This guide shows you how to ..." (they do; keep).

Verification: `check-prose.sh` on the directory shows no new rule hits; sphinx warning count unchanged; every `{ref}` resolves (no `xref_missing` warning).

### Task 7: Diataxis cleanup and deduplication, reference and explanation pages

Model: most capable. Files: `docs/sources/reference/storage-api.md`, `docs/sources/reference/state-processor-api.md`, `docs/sources/reference/schema.md`, `docs/sources/explanation/architecture.md`, `docs/sources/explanation/why-postgresql-jsonb.md`, `docs/sources/index.md`.

Read the Reference and Explanation sections of `references/diataxis.md` (path in Task 5). Reference states facts; rationale lives in explanation; a fact lives in one place and is linked from the others.

1. **Rationale out of reference.**
   - `storage-api.md` `register_prefetch_refs_expr`: keep signature, the one-line description, the "Call with `None` to disable" sentence, and the example; replace the paragraphs about N+1, cascade and plone-pgcatalog's choice with one sentence linking `` {ref}`performance-refs-prefetch` ``.
   - `storage-api.md` `load_multiple`: delete "This method is the foundation ..." through "... 1 batch query."; add one sentence linking `` {ref}`performance-batch-object-loading` ``.
   - `storage-api.md` `defer_startup_action`: delete the paragraph explaining the REPEATABLE READ deadlock; keep the facts Task 1 wrote and the example; add one sentence linking `` {ref}`startup-ddl-gate` ``.
   - `state-processor-api.md` "DDL application": reduce to the facts (when the queue runs, the lock timeouts, requeue on `OperationalError`) and one link to `` {ref}`startup-ddl-gate` ``.
   - `schema.md` "Schema installation": delete "to avoid `ACCESS EXCLUSIVE` locks that would block ..." rationale; keep the numbered sequence; link `` {ref}`startup-ddl-gate` `` for the deferred processor DDL.
2. **Stale duplicate in architecture.** `architecture.md` "Read path" (lines 90-105) still describes a single per-instance 16 MB LRU. Rewrite the section as the L1 / L2 / PostgreSQL sequence in four or five sentences and link `` {ref}`cache-tiers` `` for the details; do not duplicate `caching.md`.
3. **Pack speedup numbers.** `index.md:24` "(15-28x faster than RelStorage)", `architecture.md:244` "13-28x faster", `why-postgresql-jsonb.md:71` "13-28x faster": remove the numeric claims; each sentence links `` {ref}`performance-pack-and-garbage-collection` `` instead. `performance.md` keeps the measured table.
4. **Endings and filler.** `why-postgresql-jsonb.md`: delete the sentence "This deserves its own emphasis because it is often overlooked."; delete the closing two sentences at lines 146-147 so the page ends with the last ecosystem bullet.
5. **Trust model.** `state-processor-api.md` "Security" section is the canonical statement; make it mandatory-language reference ("You must register only processors from trusted, audited code."). `security.md` keeps its explanation. Nothing else to change here (Task 6 handled the how-to).

Verification: `check-prose.sh` shows no new rule hits on these files; every `{ref}` resolves; sphinx warning count unchanged.

### Task 8: Sentence rewrite, performance.md

Model: standard. File: `docs/sources/explanation/performance.md`.

Read the "Sentence construction" section of the SKILL.md (path at the top of this plan) first. Then run `check-prose.sh` on the file and rewrite every reported line until the script prints `clean`.

Rules of the rewrite: split joined clauses into sentences; cause in one sentence, effect in the next; replace "X, not Y" with the positive half (keep a separate "Do not ..." sentence only where the reader needs the warning); no sentence over 25 words; replace em dashes and ` -- ` with a comma, a colon, or a new sentence; replace banned words (`ensure`, `robust`, `simply`, ...). Keep every number, every identifier, every version and every causal claim. Do not touch tables or code blocks. Do not add or remove facts.

Verification: `check-prose.sh docs/sources/explanation/performance.md` prints `clean`; sphinx warning count unchanged.

### Task 9: Sentence rewrite, architecture.md

Model: standard. File: `docs/sources/explanation/architecture.md`. Same rules and verification as Task 8.

### Task 10: Sentence rewrite, caching.md

Model: standard. File: `docs/sources/explanation/caching.md`. Same rules and verification as Task 8.

### Task 11: Sentence rewrite, security.md and why-postgresql-jsonb.md

Model: standard. Files: `docs/sources/explanation/security.md`, `docs/sources/explanation/why-postgresql-jsonb.md`. Same rules and verification as Task 8, both files `clean`.

### Task 12: Sentence rewrite, all remaining pages

Model: standard. Files: every page under `docs/sources/` except `specs/` and the five files of Tasks 8 to 11. Same rules as Task 8.

Verification: `bash check-prose.sh $(find docs/sources -name '*.md' -not -path '*/specs/*')` prints `clean` for all 25 files; sphinx warning count unchanged.

### Task 13: Metadata, prose check, build strictness, changelog

Model: standard. Files: `docs/sources/index.md`, `docs/sources/tutorials/index.md`, `docs/sources/how-to/index.md`, `docs/sources/reference/index.md`, `docs/sources/explanation/index.md`, `docs/sources/conf.py`, new `docs/check-prose.sh`, new `docs/Makefile`, `.github/workflows/docs.yaml`, `CHANGES.md`, `.gitignore`.

1. **Metadata.** Add the `myst.html_meta` frontmatter block from SKILL.md ("Page metadata") to the five index pages, as the very first thing in the file (before the `<!-- diataxis: ... -->` comment). `description` and `property=og:description` identical, one sentence; `property=og:title` the page title; `keywords` "ZODB, PostgreSQL, JSONB, Plone, zodb-pgjsonb" plus one or two page words.
2. **Vendor the prose check.** Copy `/home/jensens/.claude/plugins/cache/plone-doc-style-skill/plone-doc-style/0.5.0/skills/plone-doc-style/scripts/check-prose.sh` to `docs/check-prose.sh` unchanged except a comment block under the shebang: source `https://github.com/plone/plone-doc-style-skill`, version 0.5.0, MIT license. Mark it executable.
3. **Makefile.** New `docs/Makefile` with targets `venv` (creates `docs/.venv` with `uv venv` and installs the same packages as the CI workflow), `html` (`.venv/bin/sphinx-build -W --keep-going sources html`), `linkcheck` (`-b linkcheck`), `prose` (`bash check-prose.sh $(find sources -name '*.md' -not -path '*/specs/*')`), `clean`. `html` and `linkcheck` depend on `venv`. Add `docs/.venv/` to `.gitignore`.
4. **conf.py.** `exclude_patterns = ["specs"]` so the design documents under `docs/sources/specs/` stop producing toctree and header warnings. Set `linkcheck_ignore` to an empty list placeholder only if needed by the Makefile (otherwise leave).
5. **CI.** In `.github/workflows/docs.yaml`: build with `-W --keep-going`; add a step before the build that runs `bash check-prose.sh` over the pages (same `find` as the Makefile); add a `pull_request` trigger with the same `paths`; guard the `deploy` job with `if: github.event_name == 'push'`; make the concurrency group `pages-${{ github.event_name }}-${{ github.ref }}`.
6. **CHANGES.md.** Add a `## unreleased` section at the top (format of earlier unreleased sections in the git history: `## unreleased` then `### Documentation`) with one entry describing the overhaul: Plone documentation style (one sentence per line, labelled sections, lexers, version directives), Diataxis cleanup of tutorials and guides, corrected stale facts (constructor signature, deprecated `cache-local-mb`, DDL lock timeouts, `zodb-convert`), prose check and strict build in CI. Also convert the two relative links in CHANGES.md that point at `docs/superpowers/specs/...` files into absolute `https://github.com/bluedynamics/zodb-pgjsonb/blob/main/docs/superpowers/specs/<file>` links; they are the two remaining `xref_missing` build warnings.
7. **Verify.** `make -C docs prose` prints `clean`; `make -C docs html` exits 0 with zero warnings (first create the venv; if the sandbox blocks `uv venv` inside `docs/`, run the equivalent with the scratchpad venv and report that `make venv` was not exercised).

Verification: zero sphinx warnings with `-W`; prose check clean; `git status` shows no untracked build artifacts.
