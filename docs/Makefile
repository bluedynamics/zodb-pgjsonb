SPHINXBUILD = .venv/bin/sphinx-build
SOURCES = sources
BUILD = html

.PHONY: venv html linkcheck prose clean

venv:
	test -x .venv/bin/sphinx-build || ( \
		uv venv .venv && \
		uv pip install --python .venv/bin/python sphinx sphinx-autobuild \
			myst_parser sphinxcontrib.mermaid shibuya sphinx-design sphinx-copybutton )

html: venv
	$(SPHINXBUILD) -W --keep-going $(SOURCES) $(BUILD)

linkcheck: venv
	$(SPHINXBUILD) -W --keep-going -b linkcheck $(SOURCES) $(BUILD)/linkcheck

# reference/changelog.md only includes CHANGES.md, which carries the H1,
# so the h1-count check would flag it forever.
prose:
	bash check-prose.sh $(shell find sources -name '*.md' -not -path '*/specs/*' -not -name changelog.md)

clean:
	rm -rf $(BUILD)
