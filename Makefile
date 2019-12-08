
bash =  
docker = docker run -it --rm 2kodevs:scrapper
target = bash
run = $($(target))
folder = local_data

edit: ## Open the Makefile in editor
	code Makefile

python-image: ## Generate a docker image with all python dependencies
	docker build python-image/ -t 2kodevs:scrapper-deps

project-image: ## Generate a docker image with the project files
	docker build python-image/ -t 2kodevs:scrapper

dispatcher: ## Run a dispatcher with default params in <target>
	$(run) python dispatcher.py

dispatcher-remote: ## Run a dispatcher with default params and with seed address atached in <target>
	$(run) python dispatcher.py -s 127.0.0.1:8101

seed: ## Run a scrapper with default params and seeder flag on in <target>
	$(run) python seed.py

worker: ## Run a scrapper with default params in <target>
	$(run) python scrapper.py

clean: ## Open the Makefile in editor
	rm downloads/*

%: ## If command name exists in folder, it's content is copied to urls
	cat $(folder)/$@ > urls

help: ## List available commands
	@grep -E '^[a-zA-Z_-%]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
