flags=

edit: ## Open the Makefile in editor
	gedit Makefile

dispacher: ## Run a dispacher with default params
	python dispacher.py

seeder: ## Run a scrapper with default params and seeder flag on
	python scrapper.py -s

worker: ## Run a scrapper with default params
	python scrapper.py

run-%: %.py ## Run a custom node with custom flags
	python $< $(flags)

help: ## List available commands
	@grep -E '^[a-zA-Z_-%]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
