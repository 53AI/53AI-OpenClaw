#!/bin/bash
#
# 53AI OpenClaw 插件发布脚本
# 用法: ./scripts/publish.sh [patch|minor|major|<version>] [--otp=<code>]
#
# 示例:
#   ./scripts/publish.sh          # 发布当前版本（不升级版本号）
#   ./scripts/publish.sh patch    # 升级补丁版本 (1.0.0 -> 1.0.1)
#   ./scripts/publish.sh minor    # 升级次版本 (1.0.0 -> 1.1.0)
#   ./scripts/publish.sh major    # 升级主版本 (1.0.0 -> 2.0.0)
#   ./scripts/publish.sh 2.1.0    # 指定版本号
#   ./scripts/publish.sh patch --otp=123456  # 使用 OTP 发布
#

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 项目根目录
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# 全局变量
OTP_CODE=""

# 打印带颜色的消息
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# 检查命令是否存在
check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "$1 未安装，请先安装"
        exit 1
    fi
}

# 检查 npm 登录状态
check_npm_auth() {
    log_info "检查 npm 登录状态..."
    
    # 检查是否有 npm token
    if [ -f ~/.npmrc ]; then
        if grep -q "//registry.npmjs.org/:_authToken" ~/.npmrc 2>/dev/null; then
            log_success "已找到 npm 认证信息"
            return 0
        fi
    fi
    
    # 尝试检查登录状态
    local npm_user
    npm_user=$(npm whoami 2>/dev/null || echo "")
    
    if [ -n "$npm_user" ]; then
        log_success "已登录 npm 用户: $npm_user"
    else
        log_error "未登录 npm，请先运行: npm login"
        log_info "如果是首次发布 @53ai 作用域包，请确保有权限或使用: npm login --registry=https://registry.npmjs.org/"
        exit 1
    fi
}

# 检查工作目录状态
check_git_status() {
    log_info "检查 git 工作目录状态..."
    
    if [ -d ".git" ]; then
        local status
        status=$(git status --porcelain)
        
        if [ -n "$status" ]; then
            log_warn "工作目录有未提交的更改:"
            echo "$status"
            echo ""
            read -p "是否继续发布? (y/N) " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                log_info "发布已取消"
                exit 0
            fi
        else
            log_success "工作目录干净"
        fi
    else
        log_warn "不是 git 仓库，跳过检查"
    fi
}

# 运行构建
run_build() {
    log_info "清理构建产物..."
    npm run clean
    
    log_info "构建项目..."
    npm run build
    
    if [ ! -d "dist" ] || [ ! -f "dist/index.esm.js" ]; then
        log_error "构建失败: dist 目录不存在"
        exit 1
    fi
    
    log_success "构建完成"
}

# 运行测试（如果有）
run_tests() {
    if [ -f "package.json" ] && grep -q '"test"' package.json; then
        log_info "运行测试..."
        npm test || {
            log_error "测试失败"
            exit 1
        }
        log_success "测试通过"
    else
        log_info "没有配置测试脚本，跳过"
    fi
}

# 更新版本号
update_version() {
    local version_type="$1"
    
    if [ -n "$version_type" ]; then
        log_info "更新版本号 ($version_type)..."
        
        local old_version
        old_version=$(node -p "require('./package.json').version")
        
        if [[ "$version_type" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            # 直接指定版本号
            npm version "$version_type" --no-git-tag-version
        else
            # 使用 semver 关键字
            npm version "$version_type" --no-git-tag-version
        fi
        
        local new_version
        new_version=$(node -p "require('./package.json').version")
        
        log_success "版本号: $old_version -> $new_version"
    else
        local current_version
        current_version=$(node -p "require('./package.json').version")
        log_info "保持当前版本: $current_version"
    fi
}

# 发布到 npm
publish_npm() {
    local current_version
    current_version=$(node -p "require('./package.json').version")
    
    log_info "准备发布 @53ai/53ai-openclaw@$current_version 到 npm..."
    
    # 显示将要发布的文件
    log_info "将要发布的文件:"
    npm pack --dry-run 2>&1 | head -20
    
    echo ""
    read -p "确认发布? (y/N) " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "发布已取消"
        exit 0
    fi
    
    # 发布
    log_info "发布中..."
    
    # 构建 npm publish 命令
    local publish_cmd="npm publish --access public"
    
    # 如果提供了 OTP，添加到命令中
    if [ -n "$OTP_CODE" ]; then
        publish_cmd="$publish_cmd --otp=$OTP_CODE"
        log_info "使用 OTP 发布..."
    fi
    
    # 执行发布
    if $publish_cmd; then
        log_success "发布成功! 🎉"
        log_info "查看: https://www.npmjs.com/package/@53ai/53ai-openclaw"
    else
        log_error "发布失败"
        log_warn "如果遇到 2FA 错误，请使用: $0 $@ --otp=<你的OTP码>"
        exit 1
    fi
}

# Git 提交和标签
git_commit_tag() {
    if [ ! -d ".git" ]; then
        return 0
    fi
    
    local current_version
    current_version=$(node -p "require('./package.json').version")
    
    log_info "创建 git 提交和标签..."
    
    # 检查是否有更改需要提交
    local status
    status=$(git status --porcelain)
    
    if [ -n "$status" ]; then
        git add package.json package-lock.json CHANGELOG.md 2>/dev/null || true
        git commit -m "chore: release v$current_version"
    fi
    
    # 创建标签
    if ! git rev-parse "v$current_version" >/dev/null 2>&1; then
        git tag -a "v$current_version" -m "Release v$current_version"
        log_success "创建标签: v$current_version"
    fi
}

# 显示使用帮助
show_help() {
    echo "用法: $0 [选项] [--otp=<code>]"
    echo ""
    echo "选项:"
    echo "  patch           升级补丁版本 (1.0.0 -> 1.0.1)"
    echo "  minor           升级次版本 (1.0.0 -> 1.1.0)"
    echo "  major           升级主版本 (1.0.0 -> 2.0.0)"
    echo "  <version>       指定版本号 (如: 2.1.0)"
    echo "  --otp=<code>    提供双因素认证 OTP 码"
    echo "  --help, -h      显示帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                      # 发布当前版本"
    echo "  $0 patch                # 升级补丁版本并发布"
    echo "  $0 minor                # 升级次版本并发布"
    echo "  $0 2.0.0                # 指定版本号并发布"
    echo "  $0 patch --otp=123456   # 使用 OTP 发布"
    echo ""
    echo "关于 2FA/OTP:"
    echo "  如果你的 npm 账户启用了双因素认证，发布时需要提供 OTP 码。"
    echo "  OTP 码可以从你的认证器应用（如 Google Authenticator）获取。"
}

# 解析参数
parse_args() {
    local version_arg=""
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --otp=*)
                OTP_CODE="${1#*=}"
                shift
                ;;
            --otp)
                OTP_CODE="$2"
                shift 2
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                if [ -z "$version_arg" ]; then
                    version_arg="$1"
                fi
                shift
                ;;
        esac
    done
    
    # 返回版本参数（通过全局变量）
    VERSION_TYPE="$version_arg"
}

# 主流程
main() {
    # 解析参数
    parse_args "$@"
    
    echo ""
    echo "========================================"
    echo "   53AI OpenClaw 插件发布脚本"
    echo "========================================"
    echo ""
    
    # 检查必要命令
    check_command node
    check_command npm
    
    # 执行发布流程
    check_npm_auth
    check_git_status
    run_tests
    update_version "$VERSION_TYPE"
    run_build
    publish_npm
    git_commit_tag
    
    echo ""
    log_success "发布流程完成!"
    echo ""
}

# 执行主流程
main "$@"
