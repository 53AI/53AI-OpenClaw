export const CHANNEL_ID = "53aihub";

// ============================================================================
// WebSocket 配置
// ============================================================================

/** 默认 WebSocket URL */
export const DEFAULT_WS_URL = "ws://localhost:8080/ws";

/** 心跳间隔（毫秒） */
export const WS_HEARTBEAT_INTERVAL_MS = 15000;

/** 
 * 最大重连次数
 * 设为 60 次，配合指数退避（最大 30 秒），总重连时间约 30 分钟
 */
export const WS_MAX_RECONNECT_ATTEMPTS = 60;

/** 重连基础延迟（毫秒） - 指数退避的起始值 */
export const WS_RECONNECT_BASE_DELAY_MS = 1000;

// ============================================================================
// 消息处理配置
// ============================================================================

/** 文本分块限制 */
export const TEXT_CHUNK_LIMIT = 4000;

/**
 * 请求分析软阈值（毫秒）
 * 超过该时间后只告警并继续等待，不直接中断长分析任务。
 */
export const REQUEST_ANALYSIS_TIMEOUT_MS = 600000;

/**
 * 读取请求分析软阈值的运行时覆盖值。
 * 仅用于本地或特定部署快速调参；未设置时回退到默认值。
 */
export function getRequestAnalysisTimeoutMs(): number {
  const raw = process.env.OPENCLAW_REQUEST_ANALYSIS_TIMEOUT_MS?.trim();
  if (!raw) {
    return REQUEST_ANALYSIS_TIMEOUT_MS;
  }

  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return REQUEST_ANALYSIS_TIMEOUT_MS;
  }

  return parsed;
}

/**
 * 消息处理超时（毫秒）
 * 保留旧常量名作为兼容别名，避免其他模块后续仍引用时出问题。
 */
export const MESSAGE_PROCESS_TIMEOUT_MS = REQUEST_ANALYSIS_TIMEOUT_MS;

/**
 * 消息缓存 TTL（毫秒）
 * 设为 35 分钟，确保覆盖最大重连时间（约 28.5 分钟）+ 缓冲
 * @see WS_MAX_RECONNECT_ATTEMPTS - 60 次，指数退避最大 30s
 */
export const MESSAGE_CACHE_TTL_MS = 35 * 60 * 1000;

// ============================================================================
// 媒体处理配置
// ============================================================================

/** 默认媒体大小上限（MB） */
export const DEFAULT_MEDIA_MAX_MB = 20;

/** 图片下载超时（毫秒） */
export const IMAGE_DOWNLOAD_TIMEOUT_MS = 30000;

/** 文件下载超时（毫秒） */
export const FILE_DOWNLOAD_TIMEOUT_MS = 60000;

/** 回复发送超时（毫秒） */
export const REPLY_SEND_TIMEOUT_MS = 10000;

/** 仅包含图片时的消息占位符 */
export const MEDIA_IMAGE_PLACEHOLDER = "<media:image>";

/** 仅包含文件时的消息占位符 */
export const MEDIA_DOCUMENT_PLACEHOLDER = "<media:document>";

// ============================================================================
// 状态管理配置
// ============================================================================

/** 消息状态 TTL（毫秒） */
export const MESSAGE_STATE_TTL_MS = 5 * 60 * 1000;

/** 消息状态清理间隔（毫秒） */
export const MESSAGE_STATE_CLEANUP_INTERVAL_MS = 60 * 1000;

/** 消息状态最大条目数 */
export const MESSAGE_STATE_MAX_SIZE = 1000;

// ============================================================================
// 持久化配置
// ============================================================================

/** 持久化目录名称 */
export const PERSISTENCE_DIR_NAME = ".53aihub_store";

/** ReqId 存储文件名 */
export const REQID_STORE_FILENAME = "reqid_map.json";

/** ReqId 刷写防抖时间（毫秒） */
export const REQID_FLUSH_DEBOUNCE_MS = 2000;

/** ReqId 最大条目数 */
export const REQID_MAX_ENTRIES = 5000;

export const THINKING_MESSAGE = "🤔 正在思考中...";

// ============================================================================
// 默认策略
// ============================================================================

export const DEFAULT_ACCESS_POLICY = "open";
