FROM golang:1.23-alpine AS build
WORKDIR /app
COPY . .
RUN go build -o events .

FROM alpine:3.20
WORKDIR /app
COPY --from=build /app/events /app/events
EXPOSE 8082
ENV EVENTS_PORT=8082
CMD ["/app/events"]