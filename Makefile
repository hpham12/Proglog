compile:
	protoc api/v1/*.proto \
	--go_out=. \
	--go_out=paths=source_relative \
	--proto_path=.

test:
	go test -race ./internal/log
