FROM postgres:18

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV CARGO_PGRX_VERSION=0.16.1
ENV PG_MAJOR=18
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH=/usr/local/cargo/bin:${PATH}

# Dependências de compilação + headers do PostgreSQL
RUN apt-get update && apt-get install -y --no-install-recommends \
	git \
	curl \
	ca-certificates \
	pkg-config \
	make \
	gcc \
	g++ \
	clang \
	llvm-dev \
	libclang-dev \
	postgresql-server-dev-18 \
	postgresql-18-pg-hint-plan \
	&& rm -rf /var/lib/apt/lists/*

# Instalar Rust (rustup) e cargo-pgrx
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal \
	&& . "$CARGO_HOME/env" \
	&& rustup default stable \
	&& cargo install --force --locked cargo-pgrx@"${CARGO_PGRX_VERSION}"


# Clonar, configurar pgrx e instalar pg_parquet
RUN . "$CARGO_HOME/env" \
	&& git clone --depth 1 https://github.com/CrunchyData/pg_parquet.git /tmp/pg_parquet \
	&& cd /tmp/pg_parquet \
	&& cargo pgrx init --pg"${PG_MAJOR}" $(which pg_config) \
	&& cargo pgrx install --release --features pg"${PG_MAJOR}" \
	&& rm -rf /tmp/pg_parquet ~/.cargo/registry ~/.cargo/git




# Configurar shared_preload_libraries no template do postgresql.conf
RUN echo "shared_preload_libraries = 'pg_parquet'" >> /usr/share/postgresql/postgresql.conf.sample

# Preparar dados
RUN mkdir -p /data/tpch
COPY ./data /data
COPY ./postgresStats/populate.sql /docker-entrypoint-initdb.d/populate.sql

# Ajustar permissões
RUN chown -R postgres:postgres /data