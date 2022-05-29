# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Load assets from PVDB assets table with type FRED

### Changed

### Deprecated

### Removed
- Ability to specify assets in configuration file

### Fixed
- Stop processing quotes for current asset when an error is received

### Security

## [0.3.0] - 2022-05-29
### Added
- Load assets from PVDB assets table with type FRED

### Removed
- Ability to specify assets in configuration file

### Fixed
- Stop processing quotes for current asset when an error is received

## [0.2.0] - 2022-05-26
### Added
- Check PVDB history and ensure that each trading day has a value for all FRED measurements.

### Changed
- Always set split factor in quote to 1

## [0.1.0] - 2022-05-20
### Added
- Download economic indicators from FRED
- Save economic indicators to backblaze and database

[Unreleased]: https://github.com/penny-vault/import-fred/compare/v0.3.0...HEAD
[0.2.0]: https://github.com/penny-vault/import-fred/compare/v0.2.0...0.3.0
[0.2.0]: https://github.com/penny-vault/import-fred/compare/v0.1.0...0.2.0
[0.1.0]: https://github.com/penny-vault/import-fred/releases/tag/v0.1.0