.PHONY: generate lint-proto test

BUF_IMAGE := bufbuild/buf:1.47.2

generate:
	docker run --rm --user "$$(id -u):$$(id -g)" -e HOME=/tmp -e XDG_CACHE_HOME=/tmp/buf -v "$$(pwd):/workspace" -w /workspace $(BUF_IMAGE) generate

lint-proto:
	docker run --rm -v "$$(pwd):/workspace" -w /workspace $(BUF_IMAGE) lint

test:
	cd control-plane && go test -race ./...
	python3 -m pytest worker/tests
