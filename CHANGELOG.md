# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.0.4] - 2026-03-19

### Fixed
- 修复了 TypeScript 语法错误（重命名了以数字开头的接口名）。
- 修复了项目构建依赖问题。

### Changed
- 将内部接口重命名为 `Hub53AI` 前缀，以符合 TypeScript 标识符规范。

## [1.0.1] - 2026-03-18

### Changed
- 更新插件名称和 ID 为 `53ai-openclaw`。
- 文档更新和规范化定义。

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
