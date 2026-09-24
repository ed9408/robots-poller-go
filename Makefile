.PHONY: build-lambda build-ec2 package-lambda

build-lambda:
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap ./cmd/lambda

build-ec2:
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o ec2poller ./cmd/ec2poller

package-lambda:
	zip bootstrap.zip bootstrap