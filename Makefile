.PHONY: all

all:
	python -m compileall ddld

test:
	python -m unittest
