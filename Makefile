.PHONY: all

all:
	python -m compileall acddl

test:
	python -m unittest
