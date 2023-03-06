# Makefile: common make commands used for local development.

.PHONY: install
install:
	poetry lock --no-update && poetry install --sync