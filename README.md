# tp1-distro

[![Build Status](https://travis-ci.com/jian01/tp1-distro.svg?token=tFcmLjoZ6PFesBqLEXNZ&branch=main)](https://travis-ci.com/jian01/tp1-distro)
[![Coverage Status](https://coveralls.io/repos/github/jian01/tp1-distro/badge.svg?branch=main&t=esYVFt&service=github)](https://coveralls.io/github/jian01/tp1-distro?branch=main)

# Comandos demo

```
# Consultar backups

python run_command.py --address localhost --port 1111 --command query_backups --args '{"name": "node1", "path": "/data/cositas"}'

# Sacar tarea

python run_command.py --address localhost --port 1111 --command delete_scheduled_task --args '{"name": "node3", "path": "/data/falsa"}'

# Borrar nodo

python run_command.py --address localhost --port 1111 --command delete_node --args '{"name": "node1"}'
```