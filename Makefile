.PHONY: docker_test

docker_test:
	docker build -t changeprop-test --target test -f .pipeline/blubber.yaml .
	docker run changeprop
