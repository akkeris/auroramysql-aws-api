FROM golang:1.9-alpine
RUN mkdir -p /app
ADD server.go /app/server.go
ADD build.sh /app/build.sh
RUN chmod +x /app/build.sh
RUN apk add --no-cache git \
    && /app/build.sh \
    && apk del git
CMD ["/app/server"]
EXPOSE 3000

