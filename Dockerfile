FROM golang:1.25.4-alpine3.22 AS build

ARG GOPROXY=https://proxy.golang.org,direct
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod GOPROXY="$GOPROXY" go mod download

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    GOPROXY="$GOPROXY" CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/temporaless ./cmd/temporaless

FROM alpine:3.22

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chmod=0555 /out/temporaless /usr/local/bin/temporaless
RUN mkdir -p /etc/temporaless /var/lib/temporaless \
    && chown 65532:65532 /var/lib/temporaless

USER 65532:65532
WORKDIR /var/lib/temporaless
VOLUME ["/var/lib/temporaless"]
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/temporaless"]
CMD ["-config", "/etc/temporaless/config.yaml"]
