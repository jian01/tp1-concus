SHELL := /bin/bash

default: run

all:

run:
	docker build -f ./backup_server/Dockerfile -t "backup_server:latest" .
	docker build -f ./sidecar/Dockerfile -t "sidecar:latest" .
	docker-compose -f docker-compose.yml up -d --build
.PHONY: run

run_demo:
	rm -rf backup_volume/data/*
	rm -rf backup_volume/database/*
	docker build -f ./backup_server/Dockerfile -t "backup_server:latest" .
	docker build -f ./sidecar/Dockerfile -t "sidecar:latest" .
	docker-compose -f docker-compose.yml up -d --build
	python run_command.py --address localhost --port 1111 --command add_node --args '{"name": "node1", "address": "node1", "port": 2222}'
	python run_command.py --address localhost --port 1111 --command add_task --args '{"name": "node1", "path": "/data/cositas", "frequency": 1}'
	python run_command.py --address localhost --port 1111 --command add_task --args '{"name": "node1", "path": "/data/falsa", "frequency": 1}'
	python run_command.py --address localhost --port 1111 --command add_node --args '{"name": "node2", "address": "node2", "port": 2222}'
	python run_command.py --address localhost --port 1111 --command add_task --args '{"name": "node2", "path": "/data/cositas", "frequency": 1}'
	python run_command.py --address localhost --port 1111 --command add_node --args '{"name": "node3", "address": "node3", "port": 2222}'
	python run_command.py --address localhost --port 1111 --command add_task --args '{"name": "node3", "path": "/data/cositas", "frequency": 1}'
	python run_command.py --address localhost --port 1111 --command add_node --args '{"name": "node4", "address": "node4", "port": 2222}'
	python run_command.py --address localhost --port 1111 --command add_task --args '{"name": "node4", "path": "/data/cositas", "frequency": 1}'
.PHONY: run_test_commands

logs:
	docker-compose -f docker-compose.yml logs -f
.PHONY: logs

stop:
	docker-compose -f docker-compose.yml stop -t 1
	docker-compose -f docker-compose.yml down
.PHONY: stop
