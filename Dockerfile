FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/gateway ./cmd/gateway \
 && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/mock ./cmd/mock

FROM alpine:3.23
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/gateway /out/mock /usr/local/bin/
COPY configs /app/configs
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["gateway"]
CMD ["-config", "/app/configs/compose.json"]
