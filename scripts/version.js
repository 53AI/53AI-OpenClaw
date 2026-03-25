/**
 * 版本更新钩子脚本
 * 在 npm version 执行后自动运行
 * 
 * 功能:
 * 1. 更新 CHANGELOG.md
 * 2. 提交更改到 git
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const ROOT_DIR = path.resolve(__dirname, '..');
const PACKAGE_JSON = path.join(ROOT_DIR, 'package.json');
const CHANGELOG = path.join(ROOT_DIR, 'CHANGELOG.md');

function getCurrentVersion() {
  const pkg = JSON.parse(fs.readFileSync(PACKAGE_JSON, 'utf-8'));
  return pkg.version;
}

function getGitUserName() {
  try {
    return execSync('git config user.name', { encoding: 'utf-8' }).trim() || 'Unknown';
  } catch {
    return 'Unknown';
  }
}

function updateChangelog(version) {
  const date = new Date().toISOString().split('T')[0];
  const changelogHeader = `## [${version}] - ${date}`;
  
  let changelog = '';
  if (fs.existsSync(CHANGELOG)) {
    changelog = fs.readFileSync(CHANGELOG, 'utf-8');
  } else {
    changelog = `# Changelog\n\nAll notable changes to this project will be documented in this file.\n\n`;
  }
  
  // 检查版本是否已存在
  if (changelog.includes(`[${version}]`)) {
    console.log(`Version ${version} already in CHANGELOG, skipping update`);
    return;
  }
  
  // 添加新版本条目
  const newEntry = `${changelogHeader}\n\n### Added\n- \n\n### Changed\n- \n\n### Fixed\n- \n\n`;
  
  // 在第一个版本条目前插入
  const firstVersionMatch = changelog.match(/## \[/);
  if (firstVersionMatch) {
    const insertPos = firstVersionMatch.index;
    changelog = changelog.slice(0, insertPos) + newEntry + changelog.slice(insertPos);
  } else {
    changelog += newEntry;
  }
  
  fs.writeFileSync(CHANGELOG, changelog);
  console.log(`Updated CHANGELOG.md for version ${version}`);
}

function gitCommit(version) {
  try {
    // 检查是否在 git 仓库中
    execSync('git rev-parse --git-dir', { stdio: 'pipe' });
    
    // 添加更改的文件
    execSync('git add package.json package-lock.json CHANGELOG.md 2>/dev/null || true', { stdio: 'inherit' });
    
    // 提交
    execSync(`git commit -m "chore: release v${version}"`, { stdio: 'inherit' });
    
    console.log(`Git commit created for v${version}`);
  } catch (err) {
    console.log('Not in a git repository or git commit skipped');
  }
}

function main() {
  const version = getCurrentVersion();
  console.log(`\n📦 Preparing release v${version}...\n`);
  
  updateChangelog(version);
  gitCommit(version);
  
  console.log(`\n✅ Release v${version} prepared successfully!\n`);
}

main();
