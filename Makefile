.PHONY: docker_test

docker_test:
	DOCKER_BUILDKIT=1 docker build -t changeprop-test --target test -f .pipeline/blubber.yaml .
	docker run changeprop-test
