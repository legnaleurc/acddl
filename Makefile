.PHONY: all

all:
	python -m compileall ddl

test:
	python -m unittest
