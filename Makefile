CONFIG_PATH=${HOME}/.Proglog
.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}
.PHONY: gencert
gencert:
	cfssl gencert \
		-initca test/ca-csr.json | cfssljson -bare ca
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="root" \
		test/client-csr.json | cfssljson -bare client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobody" \
		test/client-csr.json | cfssljson -bare nobody-client
	mv *.pem *.csr ${CONFIG_PATH}
.PHONY: test

compile:
	protoc api/v1/*.proto \
	--go_out=. \
	--go-grpc_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_opt=paths=source_relative \
	--proto_path=.

test:
	cp test/model.conf $(CONFIG_PATH)/model.conf
	cp test/policy.csv $(CONFIG_PATH)/policy.csv
	go test -race ./...

.PHONY: observable-test
observable-test:
	cp test/model.conf $(CONFIG_PATH)/model.conf
	cp test/policy.csv $(CONFIG_PATH)/policy.csv
	cd ./internal/server && go test -v -debug=true

TAG ?= 0.0.1
build-docker:
	docker build -t github.com/hpham12/proglog:$(TAG) .
