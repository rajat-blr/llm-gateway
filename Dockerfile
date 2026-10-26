FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/gateway ./cmd/gateway \
 && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/mock ./cmd/mock

FROM alpine:3.23
RUN apk add --no-cache ca-certificates \
 && mkdir -p /app/data \
 && chown 65532:65532 /app/data \
 && chmod 700 /app/data
WORKDIR /app
COPY --from=build /out/gateway /out/mock /usr/local/bin/
COPY configs /app/configs
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["gateway"]
CMD ["-config", "/app/configs/compose.json"]
