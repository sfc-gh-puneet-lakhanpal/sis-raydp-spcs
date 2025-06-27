cnf ?=config.env
include $(cnf)
export $(shell sed 's/=.*//' $(cnf))
export SS_SERVICE_NAME=raydp_base
.PHONY: help

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

login_docker: ## Build the customized osrm backend container
	snow spcs image-registry token -c raydp --format=JSON | docker login https://${IMAGE_REGISTRY} -u 0sessiontoken --password-stdin


build_docker: ## Build the customized osrm backend container
	export DOCKER_BUILDKIT=1
	docker build --platform linux/amd64 -t $(SS_SERVICE_NAME) -f Dockerfile .

all: login_docker build_docker