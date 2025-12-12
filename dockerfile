FROM alpine:edge as builder
WORKDIR /usr/src

RUN apk --no-cache add rust cargo g++ openssl openssl-dev clang \
    jq ca-certificates bash linux-headers \
    clang18 clang18-dev

RUN USER=root cargo new rt-parsing
WORKDIR /usr/src/rt-parsing

COPY Cargo.toml Cargo.lock ./
COPY currency-service ./currency-service
COPY rt-types ./rt-types
COPY rt-parsing-davi ./rt-parsing-davi

ENV ROCKSDB_LIB_DIR="/usr/lib/"
ENV SNAPPY_LIB_DIR="/usr/lib/"

RUN --mount=type=ssh \
    --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/root/.target \
    cargo fetch

COPY src ./src
COPY templates ./templates
COPY migrations ./migrations

ENV CARGO_TARGET_DIR="/root/.target"

RUN --mount=type=ssh \
    --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/root/.target \
    cargo install --locked --profile docker --path .


# ===============================
# ======   ФІНАЛЬНИЙ СТЕЙДЖ  =====
# ===============================
FROM alpine:edge

RUN apk --no-cache add openssl g++

WORKDIR /app

# двіжок
COPY --from=builder /root/.cargo/bin/rt-parsing /app/rt-parsing

# статичні файли
COPY static /app/static

# важливо: створюємо storage всередині контейнера
RUN mkdir -p /app/storage

CMD ["./rt-parsing"]
