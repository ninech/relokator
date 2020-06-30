FROM golang:alpine as builder
RUN apk --no-cache add git
WORKDIR /app
COPY . .
COPY netrc /root/.netrc
# ldflags to remove debugging tables
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o "relokator" .

FROM gcr.io/distroless/static
USER 1000:1000
COPY --from=builder /app/relokator /relokator

ENTRYPOINT ["/relokator"]
CMD ["-h"]
