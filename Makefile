
edit: ## Open the Makefile in editor
	code Makefile

dispatcher: ## Run a dispatcher with default params
	python dispatcher.py

seeder: ## Run a scrapper with default params and seeder flag on
	python scrapper.py -s

worker: ## Run a scrapper with default params
	python scrapper.py

clean: ## Open the Makefile in editor
	rm downloads/*

help: ## List available commands
	@grep -E '^[a-zA-Z_-%]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
