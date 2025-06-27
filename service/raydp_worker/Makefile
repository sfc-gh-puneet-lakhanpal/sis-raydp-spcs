cnf ?=config.env
include $(cnf)
export $(shell sed 's/=.*//' $(cnf))
export SS_SERVICE_NAME=raydp_worker
.PHONY: help

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

login_docker: ## Build the customized osrm backend container
	snow spcs image-registry token -c raydp --format=JSON | docker login https://${IMAGE_REGISTRY} -u 0sessiontoken --password-stdin


build_docker: ## Build the customized osrm backend container
	export DOCKER_BUILDKIT=1
	docker build --platform linux/amd64 -t $(SS_SERVICE_NAME) -f Dockerfile .

tag_docker: 
	docker tag $(SS_SERVICE_NAME) $(IMAGE_REGISTRY)/$(SS_SERVICE_NAME)
	
push_docker: ## tag and push the containers to the repo $(IMAGE_REGISTRY)
	docker push $(IMAGE_REGISTRY)/$(SS_SERVICE_NAME)

build_and_push_docker: login_docker build_docker tag_docker push_docker
all: build_and_push_docker