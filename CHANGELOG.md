# 📦 Changelog 
[![conventional commits](https://img.shields.io/badge/conventional%20commits-1.0.0-yellow.svg)](https://conventionalcommits.org)
[![semantic versioning](https://img.shields.io/badge/semantic%20versioning-2.0.0-green.svg)](https://semver.org)
> All notable changes to this project will be documented in this file


## [0.7.0](https://github.com/welfare-state-analytics/pyriksprot/compare/v0.6.0...v0.7.0) (2025-06-03)

### 🍕 Features

* add binary files for word frequencies and version 1.4.1 data ([d326f54](https://github.com/welfare-state-analytics/pyriksprot/commit/d326f5421836c916f40514755e0dccd7ac69485a))
* add config.yml for corpus and metadata configuration ([a03e167](https://github.com/welfare-state-analytics/pyriksprot/commit/a03e167001abcc193b5d755ac6046df917ff2b72))
* add config.yml for corpus and metadata configuration ([d0fdd39](https://github.com/welfare-state-analytics/pyriksprot/commit/d0fdd399536eb65e767fcb4b6ca8aca7792cf242))
* add make_config script and config template for corpus and metadata generation ([0197e59](https://github.com/welfare-state-analytics/pyriksprot/commit/0197e592c51b35774c4d741ffa98655a0eb8fbd7))
* add method to retrieve chamber abbreviation from protocol name ([2389280](https://github.com/welfare-state-analytics/pyriksprot/commit/238928022c15e0935d8d7a5fa453bbb904dbedfa))
* add new configuration file for version v1.4.1 ([0a907ec](https://github.com/welfare-state-analytics/pyriksprot/commit/0a907ecb16e8dd80af5f79c747ceac6e58110696))
* add option to load extra SQL scripts ([52fcb3a](https://github.com/welfare-state-analytics/pyriksprot/commit/52fcb3a31c9d24d7dde7025791deece165afaadf))
* added methods for checking git repository tag ([e98b4ab](https://github.com/welfare-state-analytics/pyriksprot/commit/e98b4ab3f020170049b459fe7c04aa18f0cd2834))
* added tag_info script (moved from tagger project) ([56c0d13](https://github.com/welfare-state-analytics/pyriksprot/commit/56c0d136d29f342a76789adfd8cb34c48d73c256))
* moved main tagging workflow  command-line interface from pyriksprot_tagger tom pyriksprot ([5bd9a28](https://github.com/welfare-state-analytics/pyriksprot/commit/5bd9a2853c6af1565082c500d5ccbe030325368c))
* set max positional arguments limit in pylint configuration ([c41aa82](https://github.com/welfare-state-analytics/pyriksprot/commit/c41aa825d3400e8d02232eb5961d5b14e7378d84))

### 🐛 Bug Fixes

* add context passing to main function in riksprot2speech script ([c72969a](https://github.com/welfare-state-analytics/pyriksprot/commit/c72969adbd4adc4c2125dbf751077b6094d4c471))
* add gzip diff handling for .csv.gz files in .gitattributes ([008f873](https://github.com/welfare-state-analytics/pyriksprot/commit/008f8730285f17391ec91c35578590a15313cc95))
* add missing newline at the end of utility.py ([49eaee4](https://github.com/welfare-state-analytics/pyriksprot/commit/49eaee430ebe92b1aed2f4776ca938cc32bea530))
* add script to slim database by dropping original, unprocessed data ([b13ae39](https://github.com/welfare-state-analytics/pyriksprot/commit/b13ae39e60905e4b0540816e511c85bbef7bba69))
* change config filename to include versions ([b751d1a](https://github.com/welfare-state-analytics/pyriksprot/commit/b751d1a53c772c0f692b677ff70a57ddfb892404))
* correct type hints and return types in sample_tagged functions ([322759f](https://github.com/welfare-state-analytics/pyriksprot/commit/322759f78f7d54abb9a7c5a50da2e3e117bc67d4))
* count page number by pb-tag instead of parsing from URL ([647bd44](https://github.com/welfare-state-analytics/pyriksprot/commit/647bd4499efe461bef49a39a0b8b463b86f529b5))
* enhance error handling and type hints in repository functions ([b903f53](https://github.com/welfare-state-analytics/pyriksprot/commit/b903f533167eb4ff7610913c6bec715a4374bc52))
* enhance error logging for missing source items ([ee61df3](https://github.com/welfare-state-analytics/pyriksprot/commit/ee61df32c5c6763578be5510d14f62e3af00a436))
* improve error message for missing schema files in subset_to_folder ([4a34db1](https://github.com/welfare-state-analytics/pyriksprot/commit/4a34db14df7c7dcae181e8ecc28d81d74f56b6e5))
* improve error message for missing test corpora ([c98beba](https://github.com/welfare-state-analytics/pyriksprot/commit/c98bebafac5a172e8ffbe3f15ba2916f1dd2c0d6))
* raise FileNotFoundError for missing source folder ([bcb9245](https://github.com/welfare-state-analytics/pyriksprot/commit/bcb92451f69e1b30b65a938ad4f31105e288614f))
* remove deprecated test data generation from Makefile ([9fc1bbf](https://github.com/welfare-state-analytics/pyriksprot/commit/9fc1bbfee2fa2031c63e2128861d9c63e7f13e20))
* remove obsolete configuration file v1.4.1.yml ([f22be0c](https://github.com/welfare-state-analytics/pyriksprot/commit/f22be0ce7ceeb9f29b63e3170575352feccb00e7))
* rename speaker_service to vrt_speaker_service for clarity and consistency ([7b2d244](https://github.com/welfare-state-analytics/pyriksprot/commit/7b2d2449cb9e1372cf2686bbe8daf8e0d377a054))
* update changed renamed parameter names ([bab4e49](https://github.com/welfare-state-analytics/pyriksprot/commit/bab4e4940246c2ca9fe2a6d4ec5cbd40bf6d0b10))
* update dev dependencies section in pyproject.toml ([405b405](https://github.com/welfare-state-analytics/pyriksprot/commit/405b405dd9a22cb1567ecca00d6ccf51413919bb))
* update log message to include full target file path in gh_store_file ([734b43b](https://github.com/welfare-state-analytics/pyriksprot/commit/734b43bf8d22a678519681facd39b77726a0d21a))
* update parameter type for test_protocol_texts_iterator to use interface.Protocol ([a352baf](https://github.com/welfare-state-analytics/pyriksprot/commit/a352baffa02d97c39cf15dcb928cfb162601fc4b))
* update poetry.lock ([3e525b6](https://github.com/welfare-state-analytics/pyriksprot/commit/3e525b6d20b0da90514a65e655ea373a3402fcb3))
* update pylint disable comments ([793e067](https://github.com/welfare-state-analytics/pyriksprot/commit/793e067cd507a3856914179a34eb30bd01b53b87))
* update type hints for fx and source_folder parameters in Codec and database functions ([7bc51f8](https://github.com/welfare-state-analytics/pyriksprot/commit/7bc51f89fd8b17cc938642b3b4cf93b73fe4997b))
* updated test metadata ([bb218aa](https://github.com/welfare-state-analytics/pyriksprot/commit/bb218aa890896faac26e8ff2c24aad8c6047051c))
* use basename of filename as protocol name, not name in XML preface (resolves [#89](https://github.com/welfare-state-analytics/pyriksprot/issues/89)) ([20757b0](https://github.com/welfare-state-analytics/pyriksprot/commit/20757b00414c194ba175737ef29c43c776f369db))
* use true Github paths in configs ([90bdee5](https://github.com/welfare-state-analytics/pyriksprot/commit/90bdee588e19535d3316e1f398b0ffb5a42acdeb))

### 🧑‍💻 Code Refactoring

* remove commented-out example arguments for cleaner code ([75db818](https://github.com/welfare-state-analytics/pyriksprot/commit/75db8182e3cd2845e783c381f79fb44db6881797))
* remove commented-out test_load_scripts function for cleaner code ([b4d1940](https://github.com/welfare-state-analytics/pyriksprot/commit/b4d19402c08676a2a8b0405af7d847e2dd591a8b))
* remove unnecessary info log for cleaner output ([292d1b3](https://github.com/welfare-state-analytics/pyriksprot/commit/292d1b38ee45ab81eface0bb85c661927ab7c810))
* reverted black formatting ([921ad54](https://github.com/welfare-state-analytics/pyriksprot/commit/921ad543216deae4b489058242aefd9e38b47db4))
* use c´lick commands instead of calling Python files ([fcb0d9a](https://github.com/welfare-state-analytics/pyriksprot/commit/fcb0d9a20a8c4d964f827ca22adab840cb01e26a))

### ✅ Tests

* allow missing tagged frames folder when running tests ([720b1af](https://github.com/welfare-state-analytics/pyriksprot/commit/720b1af9619d049b19ed2fc16ad539f013f88329))
* updated test data ([303ae6f](https://github.com/welfare-state-analytics/pyriksprot/commit/303ae6fc7c686b7faf6d55e5fb2b87ebe51962a6))

## [0.6.0](https://github.com/welfare-state-analytics/pyriksprot/compare/v0.5.0...v0.6.0) (2025-05-28)

### 🍕 Features

* add missing parties to _party_abbreviation and party tables ([2e9f342](https://github.com/welfare-state-analytics/pyriksprot/commit/2e9f34256eddc0dbef470ae79913e2199e95fdfe))
* add semantic-release configuration and GitHub Actions workflow ([9afa1de](https://github.com/welfare-state-analytics/pyriksprot/commit/9afa1de3a1bd85c6d7e9f864b1a7156f198fb825))
* add vnew party metadata (from Swedeb) ([4af6c9a](https://github.com/welfare-state-analytics/pyriksprot/commit/4af6c9a5275cd9c8e940bf16ae3e65c70449e03c))
* generate postgresql metadatabase ([464ded9](https://github.com/welfare-state-analytics/pyriksprot/commit/464ded95aed2cbd73748050ab10099659803865b))
* Resolves [#85](https://github.com/welfare-state-analytics/pyriksprot/issues/85) ([9fff60d](https://github.com/welfare-state-analytics/pyriksprot/commit/9fff60db86d2e087f57fc9f31829e98032915b88))
* use of new part data ([#82](https://github.com/welfare-state-analytics/pyriksprot/issues/82)) ([b3130ec](https://github.com/welfare-state-analytics/pyriksprot/commit/b3130ec9bfbc34b27224d543b8e6d010d998b5c6))

### 🐛 Bug Fixes

* add chamber_abbrev column to chamber table and update insert values ([b456aee](https://github.com/welfare-state-analytics/pyriksprot/commit/b456aee44ea972e81c8c2bbae74b6fdf9548c366))
* changes script name ([51ccee2](https://github.com/welfare-state-analytics/pyriksprot/commit/51ccee26e7f264ad93f891bba186313e74441f18))
* clean up whitespace and formatting in database.py ([e7494ac](https://github.com/welfare-state-analytics/pyriksprot/commit/e7494ac70841e874e7d5e1cbad98f84820c19b41))
* correct formatting in metadata_index_test.py ([8d2bf0f](https://github.com/welfare-state-analytics/pyriksprot/commit/8d2bf0f275299a4095d7e7d0af2e431eb6a0d1cd))
* correct party code for 'De moderata reformvännernas grupp' in swedeb-parties.csv ([3547e5c](https://github.com/welfare-state-analytics/pyriksprot/commit/3547e5cf388991b849aa74a051f1d3ae06d35847))
* correct typo in files_exist function name and update conditional check ([b08231b](https://github.com/welfare-state-analytics/pyriksprot/commit/b08231b89b4f685495b5377622f6120f6e4dc6c4))
* correct typo in log message and improve error handling for missing schema files ([c7b556a](https://github.com/welfare-state-analytics/pyriksprot/commit/c7b556ad51fd9ebdf31d244fdf73c0a228304c78))
* handle empty string cases for start_date and end_date ([69b382c](https://github.com/welfare-state-analytics/pyriksprot/commit/69b382cd67f9a7e10a252acc5c87e994ccfffb4c))
* improve formatting and consistency in utility.py ([ec0bcb9](https://github.com/welfare-state-analytics/pyriksprot/commit/ec0bcb9a08a0691948bbb0c5ea262484e1f26462))
* indentation error in TaggedFramePerGroupDispatcher ([90d7745](https://github.com/welfare-state-analytics/pyriksprot/commit/90d77456962e8108982de987622a3948b0020ba1))
* make tests more resiliant to changed ids ([b087c9c](https://github.com/welfare-state-analytics/pyriksprot/commit/b087c9c11ee64d769f65f2b01b5bd0a593d36021))
* remove unused import from database.py ([888c6a9](https://github.com/welfare-state-analytics/pyriksprot/commit/888c6a9e97a54904a917538695caa57286ae6a11))
* remove unused import from iterate.py ([5c25b88](https://github.com/welfare-state-analytics/pyriksprot/commit/5c25b88670d4597b548f12fa9a7990df31e1aa14))
* remove unused schema variable ([5507bca](https://github.com/welfare-state-analytics/pyriksprot/commit/5507bca4969f3d2d951b0ac1650626a0ea888177))
* remove unused variable in path_add_suffix function ([f58ed25](https://github.com/welfare-state-analytics/pyriksprot/commit/f58ed251ac5a8dea7b81c9f410e0a9202204184f))
* reorder import statements in database.py ([331a8f3](https://github.com/welfare-state-analytics/pyriksprot/commit/331a8f32d73a3fdf69b197cea57f4b6ec476309e))
* update party_id assertions in speaker info tests ([84cef26](https://github.com/welfare-state-analytics/pyriksprot/commit/84cef26a0d8062300e04bc7d9157c12c1d71cd5f))
* update pylintrc to disable 'too-many-public-methods' warning ([b153285](https://github.com/welfare-state-analytics/pyriksprot/commit/b15328533136380795da2794f835da63e2429d3d))
* update speaker info retrieval and merge strategy in tagged_speeches ([b186978](https://github.com/welfare-state-analytics/pyriksprot/commit/b1869786d78cbf8172ce219d47b5da52aee02042))
* update type hint for compose function and improve variable naming in utility.py ([22bbfbd](https://github.com/welfare-state-analytics/pyriksprot/commit/22bbfbdf59f94036fd1c6734171656b8bbebd952))
* use iterator ([0862e98](https://github.com/welfare-state-analytics/pyriksprot/commit/0862e98697e8481356905ce1d55975e753cee684))

### 🧑‍💻 Code Refactoring

* generate new test data ([#91](https://github.com/welfare-state-analytics/pyriksprot/issues/91)) ([823efc7](https://github.com/welfare-state-analytics/pyriksprot/commit/823efc7ab2fa9cbc4094cccd9a9a6207ce48553f))
* renamed metadata filename argument ([66e5468](https://github.com/welfare-state-analytics/pyriksprot/commit/66e546894dcfaea59a4ca8ffe466a658ac768411))
* renamed script ([d0cf840](https://github.com/welfare-state-analytics/pyriksprot/commit/d0cf840997349b7c80f40367ad1de000193de156))
* simplify return statement in protocol_segments function ([2da0578](https://github.com/welfare-state-analytics/pyriksprot/commit/2da057892362bb062ec4d37db353ea3287215bb6))

### ✅ Tests

* refreshed testdata ([b410914](https://github.com/welfare-state-analytics/pyriksprot/commit/b41091408b8896e08d5c9b7c10c54671adb9a019))

## [0.5.0](https://github.com/welfare-state-analytics/pyriksprot/compare/v0.4.6...v0.5.0) (2025-05-22)

### 🍕 Features

* add load_chamber_indexes function to load and validate chamber data from XML files ([c6a943b](https://github.com/welfare-state-analytics/pyriksprot/commit/c6a943bf5e34c833e27f3b23339c513b9c1f6168))
* add logging setup function to log output to file ([5554277](https://github.com/welfare-state-analytics/pyriksprot/commit/5554277685ba4dcc56aba85b16b2a6a1083c6c93))
* add utility function for Human formatted protocol name ([1c45267](https://github.com/welfare-state-analytics/pyriksprot/commit/1c45267f8951734b9778cb37fb704150214e2e6d))
* Added v1.1.0  JSON schema ([d55c2e9](https://github.com/welfare-state-analytics/pyriksprot/commit/d55c2e94e30c1f788a5dc80a6e655331c695abad))
* Filter files by file name pattern ([7642744](https://github.com/welfare-state-analytics/pyriksprot/commit/7642744ef95aeb7b025258bcd465c060ab2a11d4))
* Fix typo in comment for compute_term_frequencies function ([0113ffd](https://github.com/welfare-state-analytics/pyriksprot/commit/0113ffda938daa366af3cea01ba4a71c41433722))
* generate TEI corpus XML files when subsetting (resolves [#76](https://github.com/welfare-state-analytics/pyriksprot/issues/76)) ([9a5599d](https://github.com/welfare-state-analytics/pyriksprot/commit/9a5599dba4c689d2c379e444c858d1dd620bb388))
* improved check if test data exists ([dbcdf40](https://github.com/welfare-state-analytics/pyriksprot/commit/dbcdf40b1904614031bc2c5a5bd77679bed24052))
* improved test data generation CLI ([ec0c3f8](https://github.com/welfare-state-analytics/pyriksprot/commit/ec0c3f8b0620aa38022b015b51ffc580c42ace4b))
* log warnings to file ([4d11561](https://github.com/welfare-state-analytics/pyriksprot/commit/4d115614ae41203301aaac20d7179d57d153555f))
* Update SPEACH_CORPUS_FORMAT variable to SPEECH_CORPUS_FORMAT ([89236e0](https://github.com/welfare-state-analytics/pyriksprot/commit/89236e0056f058ca1de770ddcbb3c5960657ceeb))

### 🐛 Bug Fixes

* add type ignore comment for ConfigStore.config() call ([7b5217d](https://github.com/welfare-state-analytics/pyriksprot/commit/7b5217d85fabea03886aaeea91b3e63bac978f79))
* adjust call arguments ([da6595b](https://github.com/welfare-state-analytics/pyriksprot/commit/da6595b6edc79e00f4ae7f3aaeafca84c24f9b61))
* faulty args ([9977fcf](https://github.com/welfare-state-analytics/pyriksprot/commit/9977fcf960404d4390cb70e8d393a5a1edd91cd9))
* filename key ([560efd8](https://github.com/welfare-state-analytics/pyriksprot/commit/560efd8ebedfc1edc745e7287105b21502bd29f7))
* format ([271ed2b](https://github.com/welfare-state-analytics/pyriksprot/commit/271ed2bc7dea62f6cf332fb3087046438f6984fe))
* rename target_folder parameter to target_root_folder for clarity ([be68eb7](https://github.com/welfare-state-analytics/pyriksprot/commit/be68eb76cbe3ce240975b337f082c048686c2d08))
* rename target_folder parameter to target_root_folder for clarity in test scripts ([631ca03](https://github.com/welfare-state-analytics/pyriksprot/commit/631ca03bb29150ae5e5f68b730ab28ba2d458960))
* suppress FutureWarning and update source pattern in extract_speech_texts ([d70c96f](https://github.com/welfare-state-analytics/pyriksprot/commit/d70c96f73019b558baaaf8b83aeefb0ca4436887))
* suppress FutureWarning in multiple scripts ([1579cb0](https://github.com/welfare-state-analytics/pyriksprot/commit/1579cb033d2358fbb835c84e6cd32b596290d032))
* use true filename ([83e6555](https://github.com/welfare-state-analytics/pyriksprot/commit/83e6555016537c91a68afe148fcf912dd3a0999f))

### 🧑‍💻 Code Refactoring

* added helper properties ([a2e3064](https://github.com/welfare-state-analytics/pyriksprot/commit/a2e30644f63e352a1ce5b3e464502ed7ca912419))
* classes & file rename ([e1a9c86](https://github.com/welfare-state-analytics/pyriksprot/commit/e1a9c86ed7ebd0ca85ee6b79ae5675499d702596))
* corpus index generator ([87cf141](https://github.com/welfare-state-analytics/pyriksprot/commit/87cf141ece20b223fc1a5571bd33bd753edb6ab1))
* Download metadata files based on tables specified in `specifications` ([c497685](https://github.com/welfare-state-analytics/pyriksprot/commit/c497685d94d8ec41d4f47246c84e1f753ab144f2))
* expose argument to CLI ([06aaedb](https://github.com/welfare-state-analytics/pyriksprot/commit/06aaedba336ba0f99fe524de16b9a0c423f286fd))
* improve list Github folder ([33b60e6](https://github.com/welfare-state-analytics/pyriksprot/commit/33b60e6171c2f3b78b95d83d656741f1ddd1d7a7))
* improved meta data generation ([436aec6](https://github.com/welfare-state-analytics/pyriksprot/commit/436aec63ca1c8e86879b132f10f0cd2e12b01c7e))
* improved metadata generation ([30b70fb](https://github.com/welfare-state-analytics/pyriksprot/commit/30b70fb1c5c98530f7f66f889c9ad7456cb4f29b))
* improved test data generation ([ec7872c](https://github.com/welfare-state-analytics/pyriksprot/commit/ec7872cf90dab8d32b5c80d80454b93240cc1cdf))
* moved logic ([ee98f20](https://github.com/welfare-state-analytics/pyriksprot/commit/ee98f2026415b07ec9d9e748727b55df8a4a7670))
* ranmed mehhod ([cb2fd0f](https://github.com/welfare-state-analytics/pyriksprot/commit/cb2fd0f08cfe6a672a8b893003ad90ff234379b1))
* reduce complexity of metadata database logic ([5dc227a](https://github.com/welfare-state-analytics/pyriksprot/commit/5dc227aee5d7502f7459533aa8d7a0f585dc9178))
* reduced complexity, renames, add sep to config ([b754609](https://github.com/welfare-state-analytics/pyriksprot/commit/b7546092f352ab749ca0526f508967701a34b96b))
* Remove unused code and update file paths in riksdagen2text.py ([41d5621](https://github.com/welfare-state-analytics/pyriksprot/commit/41d56215460082f6b0cb1152dc5d1114721685d1))
* removed tags ([faeb46b](https://github.com/welfare-state-analytics/pyriksprot/commit/faeb46bf1bcf211d48b5e309d613615777471f43))
* rename, changed api call ([a7ee399](https://github.com/welfare-state-analytics/pyriksprot/commit/a7ee3993a8fec685181c88c212e5a2068a76418c))
* renamed file ([9bcee53](https://github.com/welfare-state-analytics/pyriksprot/commit/9bcee53c831db5a8f2dbf97b8f9817dbb89ff32b))
* renames ([3c769a8](https://github.com/welfare-state-analytics/pyriksprot/commit/3c769a80b241a92ebae7d4bce708ce383bfeb50d))
* replace print statements with logger calls for better logging ([67556a1](https://github.com/welfare-state-analytics/pyriksprot/commit/67556a169773aabba1f3b9bc2bdd92db3c32453a))
* Update import statement for metadata module in csv2pgsql script ([5d66ded](https://github.com/welfare-state-analytics/pyriksprot/commit/5d66dedb13c5a28ae0d4020d53d1bddef0714a18))
* use dict insted of function when decoding (codec.apply) ([63dd0f0](https://github.com/welfare-state-analytics/pyriksprot/commit/63dd0f0868d79c335680cb7a9e73584ae94fff35))

### ✅ Tests

* add new XML resources ([3c23f81](https://github.com/welfare-state-analytics/pyriksprot/commit/3c23f81d4bd01b57c8b6d15ea728f14d2ee78214))
* added missing data ([6c762b1](https://github.com/welfare-state-analytics/pyriksprot/commit/6c762b11aad96accf7bf7844c655dc229a7d6edc))
* adjusted expected values ([9efca96](https://github.com/welfare-state-analytics/pyriksprot/commit/9efca9697f8a02fc23947777232b87d4f2577632))
* don't generate new test data in unit test ([6732d85](https://github.com/welfare-state-analytics/pyriksprot/commit/6732d853e6654229154bf0c75960eff633b7a6d1))
* fixed v1.1.0 metadata ([b5c16ee](https://github.com/welfare-state-analytics/pyriksprot/commit/b5c16eefeb82ad3b0b3d2853895f748fd06741a3))
* only check that test data exist - do not generate ([773c673](https://github.com/welfare-state-analytics/pyriksprot/commit/773c673586341b9af3fbcd36bdfcb0733ab7a9b4))
* removed defunct test config ([911e077](https://github.com/welfare-state-analytics/pyriksprot/commit/911e077d2b2cff1201278092883380e924db3fc2))
* updated env file ([5c73b42](https://github.com/welfare-state-analytics/pyriksprot/commit/5c73b42ebe59b7ff3361ac766eca90eabfe4b28b))
* updated test config ([ac3fbc4](https://github.com/welfare-state-analytics/pyriksprot/commit/ac3fbc4050ec8b877f3a343af7fb8302485f6c1f))
* updated test data ([d2489ca](https://github.com/welfare-state-analytics/pyriksprot/commit/d2489ca18270b0091704c56513ab84b0dbc556c9))
