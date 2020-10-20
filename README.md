# run example

```
docker build -f ./backup_server/Dockerfile -t "backup_server:latest" .
docker build -f ./sidecar/Dockerfile -t "sidecar:latest" .
docker-compose -f docker-compose.yml up -d --build
netcat localhost 1111 <command1.json
netcat localhost 1111 <command2.json
```

# see logs

```
docker-compose -f docker-compose.yml logs -f
```

# stop

```
docker-compose -f docker-compose.yml stop -t 1
docker-compose -f docker-compose.yml down
```