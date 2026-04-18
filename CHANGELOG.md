# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Added core project docs: `README.md`, `SECURITY.md`, `RELEASE_CHECKLIST.md`, and `LICENSE`.
- Added CI workflow for automated tests and offline health checks.
- Added release workflow for tag-based verification and source artifact packaging.
- Added automated secret scanning script for local/CI quality gates.

### Changed
- Hardened credential handling in `run_app.bat` by removing hardcoded API keys.
- Upgraded PIN authentication hashing to PBKDF2 with legacy hash migration.
- Upgraded secret storage encryption in app layer to Fernet with transparent migration.

### Security
- Removed exposed key material from runtime startup script.
- Added stronger secret-management guidance through docs and env-based configuration.

