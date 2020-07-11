test:
	py.test -s -n 8 --cov=parallel --cov-report term-missing  tests/ -W ignore::pytest.PytestCollectionWarning
	rm -f .coverage*

publish:
	poetry build
	poetry publish