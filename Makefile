SHELL := /bin/bash

default: run

all:

run:
	docker build -f ./backup_server/Dockerfile -t "backup_server:latest" .
	docker build -f ./sidecar/Dockerfile -t "sidecar:latest" .
	docker-compose -f docker-compose.yml up -d --build
.PHONY: run

run_test_commands:
	netcat localhost 1111 <command1.json
	netcat localhost 1111 <command2.json
.PHONY: run_test_commands

logs:
	docker-compose -f docker-compose.yml logs -f
.PHONY: logs

stop:
	docker-compose -f docker-compose.yml stop -t 1
	docker-compose -f docker-compose.yml down
.PHONY: stop
