include .env

include ./Makefile.dev

# These need to be defined
ifndef RIKSPROT_DATA_FOLDER
$(error RIKSPROT_DATA_FOLDER is undefined)
endif

ifndef RIKSPROT_REPOSITORY_TAG
$(error RIKSPROT_REPOSITORY_TAG is undefined)
endif

# PARENT_DATA_FOLDER=$(shell dirname $(RIKSPROT_DATA_FOLDER))
METADATA_DB_NAME=riksprot_metadata.$(RIKSPROT_REPOSITORY_TAG).db

.PHONY: metadata-download
metadata-download:
	@rm -rf ./metadata/$(METADATA_DB_NAME) ./metadata/data
	@mkdir -p ./metadata/data
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py download \
		$(RIKSPROT_REPOSITORY_TAG) ./metadata/data

.PHONY: metadata-utterance-index
metadata-utterance-index:
	@mkdir -p ./metadata/data
	@rm -f ./metadata/data/protocols.csv* ./metadata/data/utterances.csv*
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py index \
		$(RIKSPROT_DATA_FOLDER)/riksdagen-corpus/corpus ./metadata/data
	@gzip ./metadata/data/protocols.csv ./metadata/data/utterances.csv

.PHONY: metadata-database
metadata-database:
	@rm -f metadata/$(METADATA_DB_NAME)
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py database \
		metadata/$(METADATA_DB_NAME) \
		--force \
		--load-index \
		--source-folder ./metadata/data \
		--scripts-folder ./metadata/sql # --branch $(RIKSPROT_REPOSITORY_TAG)
	@rm -rf $(RIKSPROT_DATA_FOLDER)/metadata
	@mkdir $(RIKSPROT_DATA_FOLDER)/metadata
	@cp -r ./metadata/data $(RIKSPROT_DATA_FOLDER)/metadata
	@cp metadata/$(METADATA_DB_NAME) $(RIKSPROT_DATA_FOLDER)/metadata

.PHONY: default-speeches-feather
default-speeches-feather:
	 PYTHONPATH=. python pyriksprot/scripts/riksprot2speech.py --compress-type feather --target-type single-id-tagged-frame-per-group --skip-stopwords --skip-text --lowercase --skip-puncts --force /data/westac/riksdagen_corpus_data/tagged_frames_v0.4.0 ./metadata/riksprot_metadata.v0.4.0.db ./data/HTX
