# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.0.0] - 2025-03-17

### Added
- Initial release
- WebSocket real-time communication support
- Direct message (DM) mode support
- Multi-modal message support (images, files)
- "Thinking" message support for user feedback
- Access policy control (open, allowlist, pairing)
- Persistent request ID storage

### Changed
- Security fix: Removed credentials from URL query parameters
- Fixed empty catch blocks with proper logging
- Fixed potential WebSocket memory leak with cleanup on all error paths
- Fixed message race condition with sequential queue processing
- Reduced max reconnect attempts from 100 to 10

### Fixed
- Type safety improvements: replaced `any` types with proper interfaces
