# Docker Setup for PostgreSQL Scripts

## Create Docker container

Start a PostgreSQL container:

```bash
docker run --name postgres-optd -e POSTGRES_USER=optd -e POSTGRES_PASSWORD=password -e POSTGRES_DB=optd -p 5432:5432 -d postgres:15
```

## Copy files into Docker

Copy SQL scripts into the container:

```bash
docker cp optdLogical.sql postgres-optd:/tmp/
docker cp pop-3.sql postgres-optd:/tmp/
docker cp run-3.sql postgres-optd:/tmp/
```

Or copy all files at once:

```bash
docker cp . postgres-optd:/tmp/optd/
```

## Execute SQL files in Docker

Connect to the PostgreSQL database inside the container and run a script:

```bash
docker exec -it postgres-optd psql -U optd -d optd -f /tmp/optdLogical.sql
```

Run multiple scripts in sequence:

```bash
docker exec -it postgres-optd psql -U optd -d optd -f /tmp/optdLogical.sql
docker exec -it postgres-optd psql -U optd -d optd -f /tmp/run-3.sql
docker exec -it postgres-optd psql -U optd -d optd -f /tmp/pop-3.sql
```


```bash
docker cp . postgres-optd:/tmp/optd/
docker exec -it postgres-optd psql -U optd -d optd -f /tmp/optd/pop-3.sql
docker exec -it postgres-optd psql -U optd -d optd -f /tmp/optd/run-3.sql
```

## Interactive SQL console



Access the PostgreSQL console inside the container:

```bash
docker exec -it postgres-optd psql -U optd -d optd
```

## View container logs

```bash
docker logs postgres-optd
```

## Stop and remove container

```bash
docker stop postgres-optd
docker rm postgres-optd
```

## Convert OPTD to memo format

Run the Python converter to generate pop-3 style memo inserts:

```bash
python converter.py optdLogical.sql -o converted-memo.sql
```

Then load the converted output into PostgreSQL:

```bash
docker exec -it postgres-optd psql -U optd -d optd -f /tmp/converted-memo.sql
```