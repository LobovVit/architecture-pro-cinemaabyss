FROM golang:1.23-alpine AS build
WORKDIR /app
COPY . .
RUN go build -o proxy .

FROM alpine:3.20
WORKDIR /app
COPY --from=build /app/proxy /app/proxy
EXPOSE 8000
ENV PROXY_PORT=8000
CMD ["/app/proxy"]