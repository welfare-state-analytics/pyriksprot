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

.PHONY: metadata
metadata: metadata-download metadata-utterance-index metadata-database
	@echo "metadata has been updated!"

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
	@echo "creating database: metadata/$(METADATA_DB_NAME)"
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
	@echo "copying $(RIKSPROT_DATA_FOLDER)/metadata to: $(RIKSPROT_DATA_FOLDER)/metadata"
	@cp metadata/$(METADATA_DB_NAME) $(RIKSPROT_DATA_FOLDER)/metadata
	@sqlite3 metadata/$(METADATA_DB_NAME) "VACUUM;"

.PHONY: metadata-database-vacuum
metadata-database-vacuum:
	@sqlite3 metadata/$(METADATA_DB_NAME) "VACUUM;"

LIGHT_METADATA_DB_NAME=riksprot_metadata.$(RIKSPROT_REPOSITORY_TAG).light.db

.PHONY: metadata-light-database
metadata-light-database:
	@cp -f metadata/$(METADATA_DB_NAME) metadata/$(LIGHT_METADATA_DB_NAME)
	@sqlite3 metadata/$(LIGHT_METADATA_DB_NAME) < ./metadata/10_make_light.sql
	@sqlite3 metadata/$(LIGHT_METADATA_DB_NAME) "VACUUM;"

ACTUAL_TAG:=v0.4.1
.PHONY: extract-speeches-to-feather
extract-speeches-to-feather:
	 PYTHONPATH=. python pyriksprot/scripts/riksprot2speech.py --compress-type feather \
	 	--target-type single-id-tagged-frame-per-group --skip-stopwords --skip-text --lowercase --skip-puncts --force \
		 	$(RIKSPROT_DATA_FOLDER)/tagged_frames_$(ACTUAL_TAG).beta \
			 	$(RIKSPROT_DATA_FOLDER)/metadata/riksprot_metadata.$(ACTUAL_TAG).db \
				 $(RIKSPROT_DATA_FOLDER)/tagged_frames_$(ACTUAL_TAG)_speeches.beta.feather

.PHONY: test-create-corpora
test-create-corpora:
	@poetry run python -c 'import tests.utility; tests.utility.ensure_test_corpora_exist(force=True)'
	@echo "Setup completed of:"
	@echo "Setup completed of:"
	@echo "  - Sample Parla-CLARIN corpus"
	@echo "  - Sample tagged frame corpus"
	@echo "  - Sample (subsetted) metadata database"

# .PHONY: test-metadata-database
# test-metadata-database:
# 	@poetry run python -c 'import tests.utility; tests.utility.create_subset_metadata_to_folder()'
# 	@echo "Setup of sample metadata database completed!"

.PHONY: test-create-speech-corpora
test-create-speech-corpora:
	@poetry run python -c 'import tests.utility; tests.utility.setup_sample_speech_corpora()'
	@echo "Setup of sample Parla-CLARIN corpus and tagged corpus completed!"

.PHONY: test-refresh-all-data
test-refresh-all-data: test-clear-sample-data test-create-corpora test-create-speech-corpora
	@echo "Done!"

test-clear-sample-data:
	@rm -rf tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)

#.ONESHELL: test-bundle-data
.PHONY: test-bundle-data
test-bundle-data:
	@mkdir -p dist && rm -f dist/riksprot_sample_testdata.$(RIKSPROT_REPOSITORY_TAG).tar.gz
	@tar --strip-components=2 -cvz -f tests/test_data/dists/riksprot_sample_testdata.$(RIKSPROT_REPOSITORY_TAG).tar.gz tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)
	@echo "Done!"
