# One image, three binaries: `api` and `worker` (docker compose picks one per
# service with `command:`), and `allinone`, which runs the api plus WORKERS
# supervised workers in a single container for one-process hosts like Render.
FROM golang:1.27-alpine AS build
WORKDIR /src
# Dependencies get their own layer so code changes do not re-download them.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/allinone ./cmd/allinone

FROM alpine:3.22
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/ /usr/local/bin/
USER 65534:65534
EXPOSE 8080 9091
CMD ["api"]
