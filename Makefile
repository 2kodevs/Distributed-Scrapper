
user = $(USER)
pwd = $(PWD)
path = /home/$(user)/scrapper-downloads
urls = "$(pwd)/urls"
bash =  
image_tag = scrapper
docker = docker run -it --rm --network host -v $(path):/home/Distributed-Scrapper/downloads -v $(urls):/home/Distributed-Scrapper/urls 2kodevs:$(image_tag)
target = bash
exec = $($(target))
folder = local_data
node = 

edit: ## Open the Makefile in editor
	code Makefile

python-docker-image: ## Generate a docker image with all python dependencies
	docker build python-image/ -t 2kodevs:scrapper-deps

project-image: ## Generate a docker image with the project files
	docker build . -t 2kodevs:scrapper

dispatcher: ## Run a dispatcher with default params in <target>
	$(exec) python dispatcher.py

dispatcher-remote: ## Run a dispatcher with default params and with seed address atached in <target>
	$(exec) python dispatcher.py -s 127.0.0.1:8101

seed: ## Run a scrapper with default params and seeder flag on in <target>
	$(exec) python seed.py

worker: ## Run a scrapper with default params in <target>
	$(exec) python scrapper.py

run: ## Run a node with params and defined <target>. Example: make run target=docker python node="seed.py -p 6666"
	$(exec) python $(node)

clean: ## Open the Makefile in editor
	rm downloads/*

%: ## If command name exists in folder, it's content is copied to urls
	cat $(folder)/$@ > urls

help: ## List available commands
	@grep -E '^[a-zA-Z_-%]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
