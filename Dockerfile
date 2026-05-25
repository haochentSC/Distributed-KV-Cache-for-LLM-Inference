# Build stage
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/cache-server ./cmd/cache-server

# Run stage
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/cache-server /cache-server
EXPOSE 50051
ENTRYPOINT ["/cache-server"]
