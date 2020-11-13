clean:
	rm *-*-*-* || true
	rm .coverage || true
	rm coverage.json || true
	rm -rf htmlcov || true
install:
	python3 -m pip install --user --upgrade .
testprepare:
	./tests/init_testdata.sh
test:
	python3 -m pytest tests/
coverage:
	coverage run --branch --source=./ -m pytest tests
	coverage html
