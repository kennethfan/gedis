# Redrock 多阶段构建：Pebble 纯 Go，CGO_ENABLED=0 打真静态二进制。
FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/redrock ./cmd/redrock

FROM scratch
COPY --from=build /out/redrock /redrock
COPY redrock.docker.toml /etc/redrock/redrock.docker.toml
VOLUME ["/data"]
EXPOSE 6380 9121
ENTRYPOINT ["/redrock", "-config", "/etc/redrock/redrock.docker.toml"]
