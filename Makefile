include .env

include ./Makefile.dev

# These need to be defined
ifndef RIKSPROT_DATA_FOLDER
$(error RIKSPROT_DATA_FOLDER is undefined)
endif

ifndef RIKSPROT_REPOSITORY_TAG
$(error RIKSPROT_REPOSITORY_TAG is undefined)
endif

VERSION=$(RIKSPROT_REPOSITORY_TAG)

METADATA_DATABASE_NAME=riksprot_metadata.$(VERSION).db

LOCAL_METADATA_FOLDER=./metadata/data/$(VERSION)
GLOBAL_METADATA_FOLDER=$(RIKSPROT_DATA_FOLDER)/metadata

SPEACH_CORPUS_FORMAT=feather

LOCAL_METADATA_DATABASE=./metadata/$(METADATA_DATABASE_NAME)
GLOBAL_METADATA_DATABASE=$(GLOBAL_METADATA_FOLDER)/$(METADATA_DATABASE_NAME)

TAGGED_FRAMES_FOLDER=$(RIKSPROT_DATA_FOLDER)/$(VERSION)/tagged_frames
TAGGED_FRAMES_SPEECHES_FOLDER=$(RIKSPROT_DATA_FOLDER)/$(VERSION)/tagged_frames_speeches.$(SPEACH_CORPUS_FORMAT)

CHECKED_OUT_TAG="$(shell git -C $(RIKSPROT_DATA_FOLDER)/riksdagen-corpus describe --tags)"

REMOTE_HOST=humlabp2.srv.its.umu.se

funkis:
ifeq ($(VERSION),$(CHECKED_OUT_TAG))
	@echo "check: using version $(VERSION) which matches checked out version"
else
	$(error repository tag $(CHECKED_OUT_TAG) and .env tag $(VERSION) mismatch)
endif

.PHONY: full
full: metadata term-frequencies speech-corpus deploy-to-global
	@echo "info: $(VERSION) metadata and test-data has been refreshed!"

########################################################################################################
# ENTRYPOINT: Main recipes that creates metadata database for current tag
########################################################################################################

.PHONY: metadata speech-index deploy-to-global

metadata: funkis metadata-download metadata-corpus-index metadata-database metadata-database-vacuum
	@sqlite3 $(LOCAL_METADATA_DATABASE) "VACUUM;"
	@echo "info: metadata $(VERSION) has been updated!"

MERGE_STRATEGY=chain
SPEECH_INDEX_TARGET_NAME=$(LOCAL_METADATA_FOLDER)/speech_index.$(MERGE_STRATEGY).$(VERSION).csv.gz

.PHONY: term-frequencies
term-frequencies:
	@echo "info: computing term frequencies for $(VERSION)"
	@PYTHONPATH=. poetry run python pyriksprot/scripts/riksprot2tfs.py \
		$(RIKSPROT_DATA_FOLDER)/riksdagen-corpus/corpus/protocols \
		$(LOCAL_METADATA_FOLDER)/term-frequencies.pkl

speech-index:
	@echo "info: creating speech index for $(VERSION)"
	@echo "  Source tagged frames: " $(TAGGED_FRAMES_FOLDER)
	@echo "       Source metadata: " $(LOCAL_METADATA_DATABASE)
	@echo "   Target speech index: " $(SPEECH_INDEX_TARGET_NAME)
	@if [ ! -f "$(LOCAL_METADATA_DATABASE)" ]; then \
		echo "error: global metadata file $(LOCAL_METADATA_DATABASE) does not exist"; \
		exit 1; \
	fi
	@PYTHONPATH=. poetry run python pyriksprot/scripts/speech_index.py \
		--merge-strategy $(MERGE_STRATEGY) \
		$(TAGGED_FRAMES_FOLDER) \
			$(SPEECH_INDEX_TARGET_NAME) \
				$(LOCAL_METADATA_DATABASE)
	@cp -f $(SPEECH_INDEX_TARGET_NAME) $(GLOBAL_METADATA_FOLDER)/
	@cp -f $(SPEECH_INDEX_TARGET_NAME) $(TAGGED_FRAMES_FOLDER)/


ACTUAL_TAG:=$(VERSION)
.PHONY: speech-corpus
speech-corpus: funkis speech-index
	@echo "info: extracting speeches in $(SPEACH_CORPUS_FORMAT) format"
	@PYTHONPATH=. poetry run python pyriksprot/scripts/riksprot2speech.py \
		--compress-type $(SPEACH_CORPUS_FORMAT) \
		--merge-strategy chain \
	 	--target-type single-id-tagged-frame-per-group \
		--skip-stopwords  \
		--skip-text \
		--lowercase \
		--skip-puncts \
		--force \
		 	$(TAGGED_FRAMES_FOLDER) \
			 	$(GLOBAL_METADATA_DATABASE) \
					$(TAGGED_FRAMES_SPEECHES_FOLDER)
	@cp -f $(SPEECH_INDEX_TARGET_NAME) $(TAGGED_FRAMES_SPEECHES_FOLDER)/
	@cp -f resources/default_speech_corpus_config.yml $(TAGGED_FRAMES_SPEECHES_FOLDER)/config.yml


.PHONY: deploy-to-global
deploy-to-global: funkis
	@echo "info: Deloying to global folder $(GLOBAL_METADATA_FOLDER)/$(VERSION)" \
		&& rm -rf $(GLOBAL_METADATA_FOLDER)/$(VERSION) \
		&& mkdir -p $(GLOBAL_METADATA_FOLDER)/$(VERSION) \
		&& cp -r $(LOCAL_METADATA_FOLDER) $(GLOBAL_METADATA_FOLDER) \
		&& cp -f $(LOCAL_METADATA_DATABASE) $(GLOBAL_METADATA_DATABASE)

.PHONY: deploy-to-remote
.ONESHELL:
deploy-to-remote: funkis
	@remote_folder=$(RIKSPROT_DATA_FOLDER)/$(VERSION) ; \
	folder_exists=$$(ssh $(USER)@$(REMOTE_HOST) 'test -d $${remote_folder}3/ && echo yes') ; \
	if [ "$${folder_exists}" == "yes" ]; then \
		echo "error: remote folder exists $(USER)@$(REMOTE_HOST):$${remote_folder} already exists. Please remove." ; \
		exit 64 ; \
	fi ;

	# echo "info: deploying to $(USER)@$(REMOTE_HOST):$(GLOBAL_METADATA_FOLDER)" \
	# 	&& scp -r $(LOCAL_METADATA_FOLDER) $(USER)@$(REMOTE_HOST):$(GLOBAL_METADATA_FOLDER)/ \
	# 	&& scp -f $(LOCAL_METADATA_DATABASE) $(USER)@$(REMOTE_HOST):$(GLOBAL_METADATA_DATABASE)/ \
	# 	&& scp -r $(RIKSPROT_DATA_FOLDER)/$(VERSION) $(USER)@$(REMOTE_HOST):$(TAGGED_FRAMES_SPEECHES_FOLDER)/$(RIKSPROT_DATA_FOLDER)/; \
	# @echo "done!"

apaas:
	@echo rsync -av --exclude 'mallet' --exclude 'gensim.model*' --exclude 'train_*z' --exclude $(RIKSPROT_DATA_FOLDER)/$(VERSION) $(USER)@$(REMOTE_HOST):$(RIKSPROT_DATA_FOLDER)/; 

########################################################################################################
# Sub-recepis follows
########################################################################################################
.PHONY: metadata metadata-download metadata-corpus-index metadata-database deploy-to-global

verify-metadata-filenames:
	@echo "info: checking metadata filenames for $(VERSION)"
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py filenames \
		$(RIKSPROT_DATA_FOLDER)/riksdagen-corpus/corpus/metadata

verify-metadata-columns:
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py columns $(VERSION)

metadata-download: funkis
	@echo "info: downloading $(VERSION) metadata"
	@rm -rf $(LOCAL_METADATA_DATABASE) $(LOCAL_METADATA_FOLDER)
	@mkdir -p $(LOCAL_METADATA_FOLDER)
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py download \
		$(VERSION) $(LOCAL_METADATA_FOLDER)
	@echo "info: metadata $(VERSION) stored in $(LOCAL_METADATA_FOLDER)"

metadata-corpus-index:
	@echo "info: generating index of protocols, utterances and speakers' notes in $(VERSION)"
	@mkdir -p ./metadata/data
	@rm -f ./metadata/data/protocols.csv* ./metadata/data/utterances.csv*
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py index \
		$(RIKSPROT_DATA_FOLDER)/riksdagen-corpus/corpus $(LOCAL_METADATA_FOLDER)


metadata-database:
	@echo "info: generating metadata/$(METADATA_DATABASE_NAME) using source $(LOCAL_METADATA_FOLDER)"
	@rm -f metadata/$(METADATA_DATABASE_NAME)
	@PYTHONPATH=. poetry run python pyriksprot/scripts/metadata2db.py database \
		metadata/$(METADATA_DATABASE_NAME) \
		--force \
		--tag "$(VERSION)" \
		--load-index \
		--source-folder $(LOCAL_METADATA_FOLDER)

# --scripts-folder ./metadata/sql


.PHONY: metadata-database-vacuum
metadata-database-vacuum:
	@sqlite3 metadata/$(METADATA_DATABASE_NAME) "VACUUM;"

LIGHT_METADATA_DB_NAME=riksprot_metadata.$(VERSION).light.db

.PHONY: metadata-light-database
metadata-light-database:
	@cp -f metadata/$(METADATA_DATABASE_NAME) metadata/$(LIGHT_METADATA_DB_NAME)
	@sqlite3 metadata/$(LIGHT_METADATA_DB_NAME) < ./metadata/10_make_light.sql
	@sqlite3 metadata/$(LIGHT_METADATA_DB_NAME) "VACUUM;"
	@cp -f metadata/$(LIGHT_METADATA_DB_NAME) $(RIKSPROT_DATA_FOLDER)/metadata

TEST_METADATA=tests/test_data/source/$(VERSION)/riksprot_metadata.db

.PHONY: metadata-dump-schema
metadata-dump-schema:
	@echo -e ".output riksprot_metadata_testdata_$(VERSION).sql\n.dump\n.exit" | sqlite3 $(TEST_METADATA)
	@echo -e ".output riksprot_metadata_testdata_schema_$(VERSION).sql\n.schema\n.exit" | sqlite3 $(TEST_METADATA)

########################################################################################################
# ENTRYPOINT: Main recipe that creates sample test data for current tag
########################################################################################################

test-data: test-data-clear test-corpus-and-metadata test-tagged-frames test-speech-corpora test-corpus-config test-data-bundle
	@echo "info: $(VERSION) test data refreshed!"

test-data-clear:
	@rm -rf tests/test_data/source/$(VERSION)
	@echo "info: $(VERSION) test data cleared."

test-corpus-and-metadata:
	@PYTHONPATH=. poetry run python ./tests/scripts/testdata.py corpus-and-metadata --force

test-tagged-frames:
	@PYTHONPATH=. poetry run python ./tests/scripts/testdata.py tagged-frames --force

test-speech-corpora:
	@PYTHONPATH=. poetry run python ./tests/scripts/testdata.py tagged-speech-corpora --force

# test-data-corpora:
# 	@poetry run python -c 'import tests.utility; tests.utility.ensure_test_corpora_exist(force=True)'
# 	@echo "info: $(VERSION) test Parla-CLARIN corpus created."
# 	@echo "info: $(VERSION) test tagged frames corpus created (if existed)."
# 	@echo "info: $(VERSION) test subsetted metadata database created."

TEST_BUNDLE_DATA_NAME=tests/test_data/dists/riksprot_sample_testdata.$(VERSION).tar.gz

test-data-bundle:
	@mkdir -p dist && rm -f $(TEST_BUNDLE_DATA_NAME)
	@cp tests/test_data/source/corpus.yml tests/test_data/source/$(VERSION)/
	@tar --strip-components=2 -cz -f $(TEST_BUNDLE_DATA_NAME) tests/test_data/source/$(VERSION)
	@echo "info: $(VERSION) created test data bundle $(TEST_BUNDLE_DATA_NAME)!"

test-corpus-config:
	@cp tests/test_data/source/corpus.yml tests/test_data/source/$(VERSION)/
	@echo "info: $(VERSION) test corpus.yml copied."

########################################################################################################
# CWB recipes
########################################################################################################

vrt-test-data:
	@PYTHONPATH=. poetry run riksprot2vrt --folder-batches tests/test_data/source/$(VERSION)/tagged_frames/ \
		tests/test_data/source/$(VERSION)/vrt/ -t protocol -t speech --batch-tag year

CWB_TARGET_FOLDER=`pwd`/tests/test_data/source/$(VERSION)/cwb
CWB_REGISTRY_ENTRY := riksprot_$(subst .,,$(VERSION))_test
CWB_CORPUS_NAME := $(shell X="${CWB_REGISTRY_ENTRY}"; echo $${X^^})

cwb-make-exists = $(if $(shell which cwb-make 2> /dev/null),true,false)

.PHONY: cwb
cwb-test-data:
	@rm -rf $(CWB_TARGET_FOLDER) && mkdir -p $(CWB_TARGET_FOLDER)
	@cwb-encode -d $(CWB_TARGET_FOLDER) -s -x -B -c utf8 \
		-F `pwd`/tests/test_data/source/$(VERSION)/vrt \
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
