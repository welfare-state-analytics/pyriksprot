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
CHECKED_OUT_TAG="$(shell git -C $(RIKSPROT_DATA_FOLDER)/riksdagen-corpus describe --tags)"

funkis:
ifeq ($(RIKSPROT_REPOSITORY_TAG),$(CHECKED_OUT_TAG))
	@echo "check: repository tag matches .env tag"
else
	$(error repository tag and .env tag mismatch)
endif

# @ if [[ "$(RIKSPROT_REPOSITORY_TAG)" == "$(CHECKED_OUT_TAG)" ]] ; then \
# 	echo "info: repository tag and .env tag are the same." ; \
#   else \
#   	echo "No!" ; \
# 	echo "$(RIKSPROT_REPOSITORY_TAG)" ; \
# 	echo "$(shell git -C /data/riksdagen_corpus_data/riksdagen-corpus describe --tags)" ; \
#   fi

.PHONY: metadata
metadata: funkis metadata-download metadata-corpus-index metadata-database
	@echo "metadata has been updated!"

.PHONY: metadata-download
metadata-download: funkis
	@rm -rf ./metadata/$(METADATA_DB_NAME) ./metadata/data/$(RIKSPROT_REPOSITORY_TAG)
	@mkdir -p ./metadata/data/$(RIKSPROT_REPOSITORY_TAG)
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py download \
		$(RIKSPROT_REPOSITORY_TAG) ./metadata/data/$(RIKSPROT_REPOSITORY_TAG)

.PHONY: metadata-corpus-index
metadata-corpus-index:
	@mkdir -p ./metadata/data
	@rm -f ./metadata/data/protocols.csv* ./metadata/data/utterances.csv*
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py index \
		$(RIKSPROT_DATA_FOLDER)/riksdagen-corpus/corpus ./metadata/data/$(RIKSPROT_REPOSITORY_TAG)
	@gzip ./metadata/data/$(RIKSPROT_REPOSITORY_TAG)/protocols.csv \
		./metadata/data/$(RIKSPROT_REPOSITORY_TAG)/utterances.csv \
		./metadata/data/$(RIKSPROT_REPOSITORY_TAG)/speaker_notes.csv


# .PHONY: metadata-speaker-notes-index
# RIKSPROT_XML_PATTERN=$(RIKSPROT_DATA_FOLDER)/riksdagen-corpus/corpus/protocols/*/*.xml
# SPEAKER_NOTE_TARGET=metadata/data/speaker_notes.csv
# metadata-speaker-notes-index:
# 	@echo "speaker_hash;speaker_note" > $(SPEAKER_NOTE_TARGET)
# 	@xmlstarlet sel -N x="http://www.tei-c.org/ns/1.0" -t -m "//x:note[@type='speaker']" \
# 		-v "concat(@n,';','\"',normalize-space(translate(text(),';','')),'\"')" -nl \
# 			$(RIKSPROT_XML_PATTERN) | sort | uniq >> $(SPEAKER_NOTE_TARGET)
# 	@gzip ./metadata/data/$(SPEAKER_NOTE_TARGET)

.PHONY: metadata-database
metadata-database:
	@echo "creating database: metadata/$(METADATA_DB_NAME)"
	@rm -f metadata/$(METADATA_DB_NAME)
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py database \
		metadata/$(METADATA_DB_NAME) \
		--force \
		--load-index \
		--source-folder ./metadata/data/$(RIKSPROT_REPOSITORY_TAG) \
		--scripts-folder ./metadata/sql

# --branch $(RIKSPROT_REPOSITORY_TAG)

.PHONY: metadata-database-deploy
metadata-database-deploy:
	@rm -rf $(RIKSPROT_DATA_FOLDER)/metadata/$(RIKSPROT_REPOSITORY_TAG)
	@mkdir -p $(RIKSPROT_DATA_FOLDER)/metadata/$(RIKSPROT_REPOSITORY_TAG)
	@cp -r ./metadata/data/$(RIKSPROT_REPOSITORY_TAG) $(RIKSPROT_DATA_FOLDER)/metadata
	@sqlite3 metadata/$(METADATA_DB_NAME) "VACUUM;"
	@cp metadata/$(METADATA_DB_NAME) $(RIKSPROT_DATA_FOLDER)/metadata
	@sqlite3 metadata/$(METADATA_DB_NAME) "VACUUM;"
	@echo "Done!"

.PHONY: metadata-database-vacuum
metadata-database-vacuum:
	@sqlite3 metadata/$(METADATA_DB_NAME) "VACUUM;"

LIGHT_METADATA_DB_NAME=riksprot_metadata.$(RIKSPROT_REPOSITORY_TAG).light.db

.PHONY: metadata-light-database
metadata-light-database:
	@cp -f metadata/$(METADATA_DB_NAME) metadata/$(LIGHT_METADATA_DB_NAME)
	@sqlite3 metadata/$(LIGHT_METADATA_DB_NAME) < ./metadata/10_make_light.sql
	@sqlite3 metadata/$(LIGHT_METADATA_DB_NAME) "VACUUM;"
	@cp -f metadata/$(LIGHT_METADATA_DB_NAME) $(RIKSPROT_DATA_FOLDER)/metadata

ACTUAL_TAG:=$(RIKSPROT_REPOSITORY_TAG)
.PHONY: extract-speeches-to-feather
extract-speeches-to-feather:
	PYTHONPATH=. python pyriksprot/scripts/riksprot2speech.py --compress-type feather \
	 	--target-type single-id-tagged-frame-per-group --skip-stopwords --skip-text --lowercase --skip-puncts --force \
		 	$(RIKSPROT_DATA_FOLDER)/tagged_frames_$(ACTUAL_TAG).beta \
			 	$(RIKSPROT_DATA_FOLDER)/metadata/riksprot_metadata.$(ACTUAL_TAG).db \
				 $(RIKSPROT_DATA_FOLDER)/tagged_frames_$(ACTUAL_TAG)_speeches.beta.feather

.PHONY: test-refresh-all-data
test-refresh-all-data: test-clear-sample-data test-create-corpora test-copy-corpus-yml test-bundle-data
	@echo "Done!"

test-clear-sample-data:
	@rm -rf tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)

.PHONY: test-create-corpora
test-create-corpora:
	@poetry run python -c 'import tests.utility; tests.utility.ensure_test_corpora_exist(force=True)'
	@echo "Setup completed of:"
	@echo "  - Sample Parla-CLARIN corpus"
	@echo "  - Sample tagged frame corpus"
	@echo "  - Sample (subsetted) metadata database"

TEST_BUNDLE_DATA_NAME=tests/test_data/dists/riksprot_sample_testdata.$(RIKSPROT_REPOSITORY_TAG).tar.gz

.PHONY: test-bundle-data
test-bundle-data:
	@mkdir -p dist && rm -f $(TEST_BUNDLE_DATA_NAME)
	@cp tests/test_data/source/corpus.yml tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)/
	@tar --strip-components=2 -cvz -f $(TEST_BUNDLE_DATA_NAME) tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)
	@echo "Done!"

.PHONY: test-copy-corpus-yml
test-copy-corpus-yml:
	@cp tests/test_data/source/corpus.yml tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)/

# .PHONY: test-metadata-database
# test-metadata-database:
# 	@poetry run python -c 'import tests.utility; tests.utility.create_subset_metadata_to_folder()'
# 	@echo "Setup of sample metadata database completed!"

# test-create-speech-corpora:
# 	@poetry run python -c 'import tests.utility; tests.utility.setup_sample_speech_corpora()'
# 	@echo "Setup of sample Parla-CLARIN corpus and tagged corpus completed!"
