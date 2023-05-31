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
METADATA_FOLDER=./metadata/data/$(RIKSPROT_REPOSITORY_TAG)
CORPUS_METADATA_FOLDER=$(RIKSPROT_DATA_FOLDER)/riksdagen-corpus/corpus/metadata
RIKSPROT_METADATA_FOLDER=$(RIKSPROT_DATA_FOLDER)/metadata
TAGGED_FRAMES_FOLDER=$(RIKSPROT_DATA_FOLDER)/tagged_frames_$(RIKSPROT_REPOSITORY_TAG)

CHECKED_OUT_TAG="$(shell git -C $(RIKSPROT_DATA_FOLDER)/riksdagen-corpus describe --tags)"

funkis:
ifeq ($(RIKSPROT_REPOSITORY_TAG),$(CHECKED_OUT_TAG))
	@echo "check: using version $(RIKSPROT_REPOSITORY_TAG) which matches checked out version"
else
	$(error repository tag $(CHECKED_OUT_TAG) and .env tag $(RIKSPROT_REPOSITORY_TAG) mismatch)
endif

refresh-tag: metadata test-data metadata-database-deploy
	@echo "info: $(RIKSPROT_REPOSITORY_TAG) metadata and test-data has been refreshed!"

########################################################################################################
# ENTRYPOINT: Main recipes that creates metadata database for current tag
########################################################################################################
.PHONY: metadata metadata-download metadata-corpus-index metadata-database metadata-database-deploy
metadata: funkis metadata-download metadata-corpus-index metadata-database metadata-database-vacuum
	@echo "info: metadata $(RIKSPROT_REPOSITORY_TAG) has been updated!"

MERGE_STRATEGY=chain
SPEECH_INDEX_TARGET_NAME=$(METADATA_FOLDER)/speech_index.$(MERGE_STRATEGY).$(RIKSPROT_REPOSITORY_TAG).csv.gz

speech-index:
	@PYTHONPATH=. poetry run python pyriksprot/scripts/speech_index.py \
		--merge-strategy $(MERGE_STRATEGY) \
		$(TAGGED_FRAMES_FOLDER) \
		$(SPEECH_INDEX_TARGET_NAME) \
		$(RIKSPROT_METADATA_FOLDER)/$(METADATA_DB_NAME)
	@cp -f $(SPEECH_INDEX_TARGET_NAME) $(RIKSPROT_METADATA_FOLDER)/
	@cp -f $(SPEECH_INDEX_TARGET_NAME) $(TAGGED_FRAMES_FOLDER)/


.PHONY: metadata-database-deploy
metadata-database-deploy:
	@echo "info: clearing existing deployed $(RIKSPROT_REPOSITORY_TAG) metadata"
	@rm -rf $(RIKSPROT_DATA_FOLDER)/metadata/$(RIKSPROT_REPOSITORY_TAG)
	@mkdir -p $(RIKSPROT_DATA_FOLDER)/metadata/$(RIKSPROT_REPOSITORY_TAG)
	@cp -r $(METADATA_FOLDER) $(RIKSPROT_DATA_FOLDER)/metadata
	@echo "info: $(METADATA_FOLDER) copied to $(RIKSPROT_DATA_FOLDER)/metadata"
	@sqlite3 metadata/$(METADATA_DB_NAME) "VACUUM;"
	@cp metadata/$(METADATA_DB_NAME) $(RIKSPROT_DATA_FOLDER)/metadata
	@echo "info: $(METADATA_DB_NAME) copied to $(RIKSPROT_DATA_FOLDER)/metadata"

########################################################################################################
# Sub-recepis follows
########################################################################################################

verify-metadata-filenames:
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py filenames $(CORPUS_METADATA_FOLDER)

verify-metadata-columns:
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py columns $(RIKSPROT_REPOSITORY_TAG)

metadata-download: funkis
	@echo "info: downloading metadata $(RIKSPROT_REPOSITORY_TAG)"
	@rm -rf ./metadata/$(METADATA_DB_NAME) $(METADATA_FOLDER)
	@mkdir -p $(METADATA_FOLDER)
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py download \
		$(RIKSPROT_REPOSITORY_TAG) $(METADATA_FOLDER)
	@echo "info: metadata $(RIKSPROT_REPOSITORY_TAG) stored in $(METADATA_FOLDER)"

metadata-corpus-index:
	@echo "info: retrieving index of $(RIKSPROT_REPOSITORY_TAG) protocols, utterances and speakers' notes"
	@mkdir -p ./metadata/data
	@rm -f ./metadata/data/protocols.csv* ./metadata/data/utterances.csv*
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py index \
		$(RIKSPROT_DATA_FOLDER)/riksdagen-corpus/corpus $(METADATA_FOLDER)


metadata-database:
	@echo "info: generating metadata/$(METADATA_DB_NAME) using source $(METADATA_FOLDER)"
	@rm -f metadata/$(METADATA_DB_NAME)
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py database \
		metadata/$(METADATA_DB_NAME) \
		--force \
		--tag "$(RIKSPROT_REPOSITORY_TAG)" \
		--load-index \
		--source-folder $(METADATA_FOLDER)

# Use this option to load metadata SQL from a specific folder (otherwise read from SQL folder in metadata package)
# --scripts-folder ./metadata/sql


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

TEST_METADATA=tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)/riksprot_metadata.db

.PHONY: metadata-dump-schema
metadata-dump-schema:
	@echo -e ".output riksprot_metadata_testdata_$(RIKSPROT_REPOSITORY_TAG).sql\n.dump\n.exit" | sqlite3 $(TEST_METADATA)
	@echo -e ".output riksprot_metadata_testdata_schema_$(RIKSPROT_REPOSITORY_TAG).sql\n.schema\n.exit" | sqlite3 $(TEST_METADATA)

########################################################################################################
# ENTRYPOINT: Main recipe that creates sample test data for current tag
########################################################################################################

test-data: test-data-clear test-create-complete-testdata test-data-corpus-config test-data-bundle
	@echo "info: $(RIKSPROT_REPOSITORY_TAG) test data refreshed!"

test-data-clear:
	@rm -rf tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)
	@echo "info: $(RIKSPROT_REPOSITORY_TAG) test data cleared."

test-create-complete-testdata:
	@PYTHONPATH=. poetry run python ./tests/scripts/testdata.py corpus-and-metadata --force

test-corpus-and-metadata:
	@PYTHONPATH=. poetry run python ./tests/scripts/testdata.py corpus-and-metadata --force

test-tagged-frames:
	@PYTHONPATH=. poetry run python ./tests/scripts/testdata.py tagged-frames --force

tagged-speech-corpora:
	@PYTHONPATH=. poetry run python ./tests/scripts/testdata.py tagged-speech-corpora --force

# test-data-corpora:
# 	@poetry run python -c 'import tests.utility; tests.utility.ensure_test_corpora_exist(force=True)'
# 	@echo "info: $(RIKSPROT_REPOSITORY_TAG) test Parla-CLARIN corpus created."
# 	@echo "info: $(RIKSPROT_REPOSITORY_TAG) test tagged frames corpus created (if existed)."
# 	@echo "info: $(RIKSPROT_REPOSITORY_TAG) test subsetted metadata database created."

TEST_BUNDLE_DATA_NAME=tests/test_data/dists/riksprot_sample_testdata.$(RIKSPROT_REPOSITORY_TAG).tar.gz

test-data-bundle:
	@mkdir -p dist && rm -f $(TEST_BUNDLE_DATA_NAME)
	@cp tests/test_data/source/corpus.yml tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)/
	@tar --strip-components=2 -cz -f $(TEST_BUNDLE_DATA_NAME) tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)
	@echo "info: $(RIKSPROT_REPOSITORY_TAG) created test data bundle $(TEST_BUNDLE_DATA_NAME)!"

test-data-corpus-config:
	@cp tests/test_data/source/corpus.yml tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)/
	@echo "info: $(RIKSPROT_REPOSITORY_TAG) test corpus.yml copied."

########################################################################################################
# CWB recipes
########################################################################################################

vrt-test-data:
	@PYTHONPATH=. poetry run riksprot2vrt --folder-batches tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)/tagged_frames/ \
		tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)/vrt/ -t protocol -t speech --batch-tag year

CWB_TARGET_FOLDER=`pwd`/tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)/cwb
CWB_REGISTRY_ENTRY := riksprot_$(subst .,,$(RIKSPROT_REPOSITORY_TAG))_test
CWB_CORPUS_NAME := $(shell X="${CWB_REGISTRY_ENTRY}"; echo $${X^^})

cwb-make-exists = $(if $(shell which cwb-make 2> /dev/null),true,false)

.PHONY: cwb
cwb-test-data:
	@rm -rf $(CWB_TARGET_FOLDER) && mkdir -p $(CWB_TARGET_FOLDER)
	@cwb-encode -d $(CWB_TARGET_FOLDER) -s -x -B -c utf8 \
		-F `pwd`/tests/test_data/source/$(RIKSPROT_REPOSITORY_TAG)/vrt \
		-R /usr/local/share/cwb/registry/$(CWB_REGISTRY_ENTRY) \
		-P lemma -P pos -P xpos \
		-S year:0+title+date \
		-S protocol:0+title+date \
		-S speech:0+id+page+title+who+date
	@if [ "$(cwb-make-exists)" == "true" ]; then \
		cwb-make -V $(CWB_CORPUS_NAME); \
	else \
		echo "info: cwb-make not found, skipping corpus creation"; \
		echo "info: run 'sudo cpan install CWB' to install the CWB/Perl toolkit"
	fi
	@cwb-describe-corpus $(CWB_CORPUS_NAME)


########################################################################################################
# ENTRYPOINT: Recipe that creates default speech corpus for current tag
########################################################################################################
ACTUAL_TAG:=$(RIKSPROT_REPOSITORY_TAG)
.PHONY: extract-speeches-to-feather
extract-speeches-to-feather:
	@PYTHONPATH=. poetry run python pyriksprot/scripts/riksprot2speech.py \
		--compress-type feather \
		--merge-strategy chain \
	 	--target-type single-id-tagged-frame-per-group \
		--skip-stopwords  \
		--skip-text \
		--lowercase \
		--skip-puncts \
		--force \
		 	$(RIKSPROT_DATA_FOLDER)/tagged_frames_$(ACTUAL_TAG) \
			 	$(RIKSPROT_DATA_FOLDER)/metadata/riksprot_metadata.$(ACTUAL_TAG).db \
				 $(RIKSPROT_DATA_FOLDER)/tagged_frames_$(ACTUAL_TAG)_speeches.feather

