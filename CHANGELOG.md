# 📦 Changelog 
[![conventional commits](https://img.shields.io/badge/conventional%20commits-1.0.0-yellow.svg)](https://conventionalcommits.org)
[![semantic versioning](https://img.shields.io/badge/semantic%20versioning-2.0.0-green.svg)](https://semver.org)
> All notable changes to this project will be documented in this file


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
