DOCKER_REGISTRY := us.gcr.io/singularities-spoon
DOCKER_REPO := $(DOCKER_REGISTRY)/data-extractor
DOCKER_IMAGE_TAG := 1.0
DOCKER_IMAGE := $(DOCKER_REPO):$(DOCKER_IMAGE_TAG)

image:
	docker build . -t $(DOCKER_IMAGE)