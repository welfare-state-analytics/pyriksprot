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

SPEECH_CORPUS_FORMAT=feather

LOCAL_METADATA_DATABASE=./metadata/$(METADATA_DATABASE_NAME)
GLOBAL_METADATA_DATABASE=$(GLOBAL_METADATA_FOLDER)/$(VERSION)/$(METADATA_DATABASE_NAME)

TAGGED_FRAMES_FOLDER=$(RIKSPROT_DATA_FOLDER)/$(VERSION)/tagged_frames
LEMMA_VRT_SPEECHES_FOLDER=$(RIKSPROT_DATA_FOLDER)/$(VERSION)/lemma_vrt_speeches.$(SPEECH_CORPUS_FORMAT)

CHECKED_OUT_TAG="$(shell git -C $(RIKSPROT_DATA_FOLDER)/riksdagen-corpus describe --tags)"

REMOTE_HOST=humlabp2.srv.its.umu.se

funkis:
ifeq ($(VERSION),$(CHECKED_OUT_TAG))
	@echo "check: using version $(VERSION) which matches checked out version"
else
	$(error repository tag $(CHECKED_OUT_TAG) and .env tag $(VERSION) mismatch)
endif

.PHONY: full
full: metadata word-frequencies lemma-vrt-speech-corpus deploy-to-global
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

.PHONY: word-frequencies
word-frequencies:
	@echo "info: computing word frequencies for $(VERSION)"
	@PYTHONPATH=. poetry run python pyriksprot/scripts/riksprot2tfs.py \
		$(RIKSPROT_DATA_FOLDER)/riksdagen-corpus/corpus/protocols \
		$(LOCAL_METADATA_FOLDER)/word-frequencies.pkl

speech-index:
	@echo "info: creating speech index for $(VERSION)"
	@echo "  Source tagged frames: " $(TAGGED_FRAMES_FOLDER)
	@echo "       Source metadata: " $(LOCAL_METADATA_DATABASE)
	@echo "   Target speech index: " $(SPEECH_INDEX_TARGET_NAME)
	@if [ ! -f "$(LOCAL_METADATA_DATABASE)" ]; then \
		echo "error: metadata file $(LOCAL_METADATA_DATABASE) does not exist"; \
		exit 1; \
	fi
	@PYTHONPATH=. poetry run python pyriksprot/scripts/speech_index.py \
		--merge-strategy $(MERGE_STRATEGY) \
		$(TAGGED_FRAMES_FOLDER) \
			$(SPEECH_INDEX_TARGET_NAME) \
				$(LOCAL_METADATA_DATABASE)
	@echo "info: done creating speech index for $(VERSION)"
	@cp -f $(SPEECH_INDEX_TARGET_NAME) $(GLOBAL_METADATA_FOLDER)/
	@cp -f $(SPEECH_INDEX_TARGET_NAME) $(TAGGED_FRAMES_FOLDER)/


.PHONY: vrt-speech-corpora
vrt-speech-corpora:
	@for tok in "all" "lemma" "text"; do \
		skip_option=""; \
		target_name="vrt_speeches.$(SPEECH_CORPUS_FORMAT)" ; \
		if [ "$${tok}" == "lemma" ]; then \
			skip_option="--skip-text"; \
			target_name="$${tok}_$${target_name}"; \
		elif [ "$${tok}" == "text" ]; then \
			skip_option="--skip-lemma"; \
			target_name="$${tok}_$${target_name}"; \
		fi; \
		echo "info: extracting VRT $${tok} speeches in $(SPEECH_CORPUS_FORMAT) format"; \
		echo "info: $${target_name}"; \
		echo awk "{gsub(CORPUS_FOLDER, $(RIKSPROT_DATA_FOLDER)/$(VERSION)/corpus/$${target_name})}1" ./resources/speech_corpus.yml; \
	done


# 		PYTHONPATH=. poetry run python pyriksprot/scripts/riksprot2speech.py \
# 			--compress-type $(SPEECH_CORPUS_FORMAT) \
# 			--merge-strategy chain \
# 			--target-type single-id-tagged-frame-per-group \
# 			$${skip_option} \
# 			--lowercase \
# 			--skip-puncts \
# 			--force \
# 				$(TAGGED_FRAMES_FOLDER) \
# 					$(LOCAL_METADATA_DATABASE) \
# 						$(RIKSPROT_DATA_FOLDER)/$(VERSION)/corpus/$${target_name} ; \
#  ./resources/speech_corpus.yml > $(RIKSPROT_DATA_FOLDER)/$(VERSION)/corpus/$${target_name}/corpus.yml ; \

plain-text-speech-corpus: funkis
	@echo "info: extracting speeches in $(SPEECH_CORPUS_FORMAT) format"
	@echo "Script(s): pyriksprot.scripts.riksprot2speech_text:main"
	@PYTHONPATH=. poetry run python pyriksprot/scripts/riksprot2speech_text.py \
		--target-type single-id-tagged-frame-per-group \
		--compress-type zip \
		--merge-strategy chain \
		--skip-size 1 \
		--multiproc-processes 1 \
		--dedent \
		--dehyphen \
		--dehyphen-folder $(LOCAL_METADATA_FOLDER) \
		--force \
			$(RIKSPROT_DATA_FOLDER)/riksdagen-corpus/corpus/protocols \
				$(LOCAL_METADATA_DATABASE) \
					$(RIKSPROT_DATA_FOLDER)/$(VERSION)/corpus/plain_text_speeches.zip

.PHONY: deploy-to-global
deploy-to-global: funkis
	@echo "info: deploying to global folder $(GLOBAL_METADATA_FOLDER)/$(VERSION)"
	@printf "  %50s => %s\n" "$(LOCAL_METADATA_FOLDER)" "$(GLOBAL_METADATA_FOLDER)"
	@printf "  %50s => %s\n" "$(LOCAL_METADATA_DATABASE)" "$(GLOBAL_METADATA_DATABASE)"
	@printf "  %50s => %s\n" "$(GLOBAL_METADATA_DATABASE)" "$(GLOBAL_METADATA_FOLDER)/$(METADATA_DATABASE_NAME) (link)"
	@printf "  %50s => %s\n" "$(LOCAL_METADATA_FOLDER)/word-frequencies.pkl" "$(GLOBAL_METADATA_FOLDER)/$(VERSION)/word-frequencies.pkl"
	@cp -r $(LOCAL_METADATA_FOLDER) $(GLOBAL_METADATA_FOLDER)  \
	   && cp -f $(LOCAL_METADATA_DATABASE) $(GLOBAL_METADATA_DATABASE) \
	   && rm -f $(GLOBAL_METADATA_FOLDER)/$(METADATA_DATABASE_NAME) \
	   && ln -s $(GLOBAL_METADATA_DATABASE) $(GLOBAL_METADATA_FOLDER)/$(METADATA_DATABASE_NAME) \
	   && if [ -f "$(LOCAL_METADATA_FOLDER)/word-frequencies.pkl" ]; then \
		      cp -f $(LOCAL_METADATA_FOLDER)/word-frequencies.pkl $(GLOBAL_METADATA_FOLDER)/$(VERSION)/word-frequencies.pkl; \
	      fi ;

.PHONY: deploy-to-remote
.ONESHELL:
deploy-metadata-to-remote: funkis
	@printf "%s\n" "info: deploying to $(USER)@$(REMOTE_HOST):$(GLOBAL_METADATA_FOLDER)"
	@printf "  %50s => %s\n" "$(LOCAL_METADATA_FOLDER)" "$(USER)@$(REMOTE_HOST):$(GLOBAL_METADATA_FOLDER)"
	@printf "  %50s => %s\n" "$(LOCAL_METADATA_DATABASE)" "$(USER)@$(REMOTE_HOST):$(GLOBAL_METADATA_DATABASE)"
	# @scp -r $(LOCAL_METADATA_FOLDER) $(USER)@$(REMOTE_HOST):$(GLOBAL_METADATA_FOLDER)/
	# @scp $(LOCAL_METADATA_DATABASE) $(USER)@$(REMOTE_HOST):$(GLOBAL_METADATA_DATABASE)
	@echo ssh $(USER)@$(REMOTE_HOST) 'ln -s $(GLOBAL_METADATA_DATABASE) $(GLOBAL_METADATA_FOLDER)/$(METADATA_DATABASE_NAME)' "
	@echo ssh $(USER)@$(REMOTE_HOST) 'ln -s $(GLOBAL_METADATA_FOLDER)/$(VERSION)/word-frequencies.pkl $(RIKSPROT_DATA_FOLDER)/word-frequencies.pkl' "

deploy-data-to-remote:
	@printf "  %50s => %s\n" "$(RIKSPROT_DATA_FOLDER)/$(VERSION)" "$(USER)@$(REMOTE_HOST):$(RIKSPROT_DATA_FOLDER)/$(VERSION)"
	@folder_exists=`ssh $(USER)@$(REMOTE_HOST) 'test -d $(RIKSPROT_DATA_FOLDER)/$(VERSION) && echo yes'` ; \
		if [ "$${folder_exists}" == "yes" ]; then \
	 		echo "error: remote folder exists $(USER)@$(REMOTE_HOST):$(RIKSPROT_DATA_FOLDER)/$(VERSION) already exists. Please remove." ; \
	 		exit 64 ; \
	 	fi ;
	@tar cf $(RIKSPROT_DATA_FOLDER)/$(VERSION).tar $(RIKSPROT_DATA_FOLDER)
	@scp $(RIKSPROT_DATA_FOLDER)/$(VERSION).tar $(USER)@$(REMOTE_HOST):$(RIKSPROT_DATA_FOLDER)/
	@rm -f $(RIKSPROT_DATA_FOLDER)/$(VERSION).tar
	@echo "info: unpack $(RIKSPROT_DATA_FOLDER)/$(VERSION).tar on recieving end"

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
		-S year:0+year+title \
		-S protocol:0+title+date \
		-S speech:0+id+page+title+who+date
	@if [ "$(cwb-make-exists)" == "true" ]; then \
		cwb-make -V $(CWB_CORPUS_NAME); \
	else \
		echo "info: cwb-make not found, skipping corpus creation"; \
		echo "info: run 'sudo cpan install CWB' to install the CWB/Perl toolkit"
	fi
	@cwb-describe-corpus $(CWB_CORPUS_NAME)
