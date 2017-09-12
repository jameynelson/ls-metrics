ls-metrics:
	CGO_ENABLED=0 go build -o ls-metrics *go

ls-metrics-linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ls-metrics *go
clean:
	rm -f ls-metrics
