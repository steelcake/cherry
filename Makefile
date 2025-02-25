build_install:
	python -m build && pip uninstall cherry-indexer -y && pip install dist/cherry_indexer-0.1.7-py3-none-any.whl
