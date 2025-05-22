include .env

SHELL := /bin/bash

DATA_FOLDER="$(shell pwd)"
PROJECT_FOLDER=$(shell bash -c 'echo "$$1" |{ p="$$1"; echo "$${p%/pyriksprot*}"; }' _ $(DATA_FOLDER))/pyriksprot
SCRIPTS_FOLDER=$(PROJECT_FOLDER)/pyriksprot/scripts/
CONFIG_FILENAME=./config_$(CORPUS_VERSION).yml
CORPUS_FOLDER=$(CORPUS_VERSION)/riksdagen-records
METADATA_FOLDER=metadata/$(METADATA_VERSION)

dataset: parlaclarin-corpus word-frequencies tag-protocols tagged-speech-corpora text-corpora dtm-corpora

$(CONFIG_FILENAME):
	@PYTHONPATH=$(PROJECT_FOLDER) python $(SCRIPTS_FOLDER)/make_config.py $(CONFIG_FILENAME) \
		--corpus-version $(CORPUS_VERSION) \
		--metadata-version $(METADATA_VERSION) \
		--root-folder . \
		--corpus-folder $(CORPUS_FOLDER) \
		--stanza-datadir /data/sparv/models/stanza

parlaclarin-corpus: $(CONFIG_FILENAME)
	@echo "Target(s): 1) subset of Parla-CLARIN corpus 2) processed metadata"
	@echo "Script(s): pyriksprot.scripts.subset-corpus:main"
	@PYTHONPATH=$(PROJECT_FOLDER) python $(SCRIPTS_FOLDER)/subset_corpus.py --skip-download $(CONFIG_FILENAME) ./protocols.txt ./ 
	@mv -f $(CONFIG_FILENAME) $(CORPUS_VERSION)/config.yml

word-frequencies:
	@echo "Target(s): global term frequencies (TF)"
	@echo "Script(s): pyriksprot.scripts.riksprot2tfs:main"
	@mkdir -p $(CORPUS_VERSION)/dehyphen
	@riksprot2tfs $(CORPUS_FOLDER) $(CORPUS_VERSION)/dehyphen/word-frequencies.pkl

tag-protocols:
	@echo "Target(s): tagged parlaclarin protocols (tagged frames)"
	@echo "Script(s): pyriksprot_tagger.scripts.tag:main"
	@pos_tag opts/tagger-config.yml $(CORPUS_FOLDER) $(CORPUS_VERSION)/tagged_frames

tagged-speech-corpora: ./opts/tagged-speeches/*.yml
	@echo "Target(s): tagged speech corpora (tagged frames)"
	@echo "Script(s): pyriksprot.scripts.riksprot2speech:main"
	@rm -rf $(CORPUS_VERSION)/speeches && mkdir -p $(CORPUS_VERSION)/speeches
	@for file in $^ ; do \
		echo " => $(CORPUS_VERSION)/speeches/$$(basename $${file%.yml})" ; \
		riksprot2speech \
			--options-filename $${file} --force \
			metadata$(CORPUS_VERSION)/tagged_frames \
				$(CORPUS_VERSION)/riksprot_metadata.db \
					$(CORPUS_VERSION)/speeches/$$(basename $${file%.yml}) ; \
    done

text-corpora: ./opts/plain-text-speeches/*.yml
	@echo "Target(s): speech text corpora (plain text)"
	@echo "Script(s): pyriksprot.scripts.riksprot2speech_text:main"
	@mkdir -p $(CORPUS_VERSION)/speeches
	@for file in $^ ; do \
		echo " => $(CORPUS_VERSION)/speeches/$$(basename $${file%.yml})" ; \
		riksprot2speech_text \
			--options-filename $${file} --force \
			$(CORPUS_VERSION)/riksdagen-records \
				metadata/$(METADATA_TAG)/riksprot_metadata.db \
					$(CORPUS_VERSION)/speeches/$$(basename $${file%.yml}).zip ; \
    done

PENELOPE_FOLDER=/home/roger/source/penelope
dtm-corpora: ./opts/dtm/*.yml
	@echo "Target(s): DTM corpora"
	@echo "Script(s): pyriksprot.scripts.riksprot2speech_text:main"
	@echo "     Note: penelope must be installed in current environment"
	@rm -rf $(CORPUS_VERSION)/dtm && mkdir -p $(CORPUS_VERSION)/dtm
	 for file in $^ ; do \
	 	tag=$$(basename $${file%.yml})
		echo " => $(CORPUS_VERSION)/speeches/$${tag}" ; \
		vectorize-id \
		    --create-subfolder \
			--options-filename $${file} \
			--corpus-source $(CORPUS_VERSION)/speeches/tagged_frames_speeches_$${tag}.feather \
			--output-folder $(CORPUS_VERSION)/dtm \
			--output-tag $${tag} ; \
    done
# poetry run -C $(PENELOPE_FOLDER) vectorize-id \

clean:
	@rm -rf $(CORPUS_VERSION)
