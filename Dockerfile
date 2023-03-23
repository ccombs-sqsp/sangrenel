FROM golang:1.19
 
WORKDIR /app
COPY . /app
RUN go build
CMD ["/app/sangrenel", "-api-version", "2.2.0"]