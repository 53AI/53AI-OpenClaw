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

/** 消息处理超时（毫秒） */
export const MESSAGE_PROCESS_TIMEOUT_MS = 120000;

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