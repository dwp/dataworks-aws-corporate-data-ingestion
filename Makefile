SHELL:=bash

aws_profile=default
aws_region=eu-west-2

default: help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

bootstrap: bootstrap-terraform get-dependencies

create-workspaces: bootstrap-terraform terraform-workspace-new

.PHONY: bootstrap
bootstrap-terraform: ## Bootstrap local environment for first use
	@make git-hooks
	pip3 install --user Jinja2 PyYAML boto3
	@{ \
		export AWS_PROFILE=$(aws_profile); \
		export AWS_REGION=$(aws_region); \
		python3 bootstrap_terraform.py; \
	}
	terraform fmt -recursive

.PHONY: git-hooks
git-hooks: ## Set up hooks in .githooks
	@git submodule update --init .githooks ; \
	git config core.hooksPath .githooks \


.PHONY: terraform-init
terraform-init: ## Run `terraform init` from repo root
	terraform init

.PHONY: terraform-plan
terraform-plan: ## Run `terraform plan` from repo root
	terraform plan

.PHONY: terraform-apply
terraform-apply: ## Run `terraform apply` from repo root
	terraform apply

.PHONY: terraform-workspace-new
terraform-workspace-new: ## Creates new Terraform workspace with Concourse remote execution
	declare -a workspace=( qa integration preprod production )  && \
	make bootstrap ; \
	cp terraform.tf workspaces.tf && \
	for i in "$${workspace[@]}" ; do \
		fly -t aws-concourse execute --config create-workspace.yml --input repo=. -v workspace="$$i" ; \
	done
	rm workspaces.tf

.PHONY: get-dependencies
get-dependencies: ## Get dependencies that are normally managed by pipeline
	@{ \
		for github_repository in emr-launcher dataworks-emr-relauncher; do \
			export REPO=$${github_repository}; \
			./get_lambda_release.sh; \
		done \
	}