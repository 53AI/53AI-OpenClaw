import type { OpenClawConfig, RuntimeEnv } from "openclaw/plugin-sdk";
import type { ResolvedAccount } from "./utils.js";

// ============================================================================
// WebSocket 消息类型
// ============================================================================

/**
 * OpenAI 兼容的消息格式
 */
export interface OpenAIChatMessage {
  role: "user" | "assistant" | "system";
  content: string | MessageContentItem[];
  name?: string;
}

/**
 * OpenAI 兼容的请求格式
 */
export interface OpenAIChatRequest {
  model?: string;
  messages: OpenAIChatMessage[];
  user?: string;
  conversation_id?: string;
  stream?: boolean;
}

/**
 * OpenAI 兼容的响应块格式
 */
export interface OpenAIChatCompletionChunk {
  id: string;
  object: "chat.completion.chunk";
  created: number;
  model: string;
  choices: Array<{
    index: number;
    delta: {
      content?: string;
      role?: string;
    };
    finish_reason: string | null;
  }>;
  error?: ResponseError;
}

/**
 * AgentHub 消息数据类型
 */
export interface AgentHubMessageData {
  toChatId?: string;
  text?: string;
  imageUrls?: string[];
  fileUrls?: string[];
  type?: string;
  msgId?: string;
  id?: string;
  chatId?: string;
  userId?: string;
  content?: string;
  quoteContent?: string;
  images?: Array<{ url?: string }>;
  files?: Array<{ url?: string }>;
  media?: {
    type: "image" | "file";
    url?: string;
    base64?: string;
    mimeType?: string;
    filename?: string;
  };
}

/**
 * WebSocket 请求/响应消息基础格式
 */
export interface AgentHubWsMessage {
  req_id: string;
  action: "chat" | "message" | "ping" | "pong";
  status: "streaming" | "done" | "error" | "final" | "thinking";
  data: OpenAIChatRequest | OpenAIChatCompletionChunk | AgentHubMessageData | null;
}

/**
 * 图片消息内容
 */
export interface ImageContent {
  /** 图片 URL */
  url?: string;
  /** 图片 base64 数据 */
  base64?: string;
  /** 图片 MIME 类型 */
  mimeType?: string;
}

/**
 * 文件消息内容
 */
export interface FileContent {
  /** 文件 URL */
  url?: string;
  /** 文件 base64 数据 */
  base64?: string;
  /** 文件名 */
  filename?: string;
  /** 文件 MIME 类型 */
  mimeType?: string;
}

/**
 * 消息内容项（支持多模态）
 */
export interface MessageContentItem {
  type: "text" | "image" | "file";
  text?: string;
  image?: ImageContent;
  file?: FileContent;
}

/**
 * 来自 Go 后端的消息
 */
export interface AgentHubIncomingMessage {
  type: string;
  msgId: string;
  chatId: string;
  userId: string;
  text: string;
  /** 图片 URL 列表 */
  imageUrls?: string[];
  /** 文件 URL 列表 */
  fileUrls?: string[];
  /** 多模态消息内容项 */
  contentItems?: MessageContentItem[];
  quoteContent?: string;
  reqId?: string;
}

/**
 * 发送给 Go 后端的消息
 */
export interface AgentHubOutgoingMessage {
  type: "reply" | "message";
  msgId?: string;
  chatId: string;
  text: string;
  streamId?: string;
  finish: boolean;
  error?: ResponseError;
  /** 媒体附件 */
  media?: {
    type: "image" | "file";
    url?: string;
    base64?: string;
    mimeType?: string;
    filename?: string;
  };
}

// ============================================================================
// 配置和运行时类型
// ============================================================================

/**
 * Monitor 配置选项
 */
export interface MonitorOptions {
  account: ResolvedAccount;
  config: OpenClawConfig;
  runtime: RuntimeEnv;
  abortSignal?: AbortSignal;
}

/**
 * 消息状态
 */
export interface MessageState {
  accumulatedText: string;
  lastSentText: string;
  streamId: string;
}

// ============================================================================
// 错误处理
// ============================================================================

/**
 * 错误码枚举
 */
export enum ErrorCode {
  // 访问控制
  ACCESS_DENIED = "ACCESS_DENIED",
  PAIRING_REQUIRED = "PAIRING_REQUIRED",
  
  // AI 服务错误
  RATE_LIMITED = "RATE_LIMITED",
  INSUFFICIENT_QUOTA = "INSUFFICIENT_QUOTA",
  MODEL_OVERLOADED = "MODEL_OVERLOADED",
  MODEL_NOT_FOUND = "MODEL_NOT_FOUND",
  
  // 请求错误
  INVALID_REQUEST = "INVALID_REQUEST",
  CONTEXT_LENGTH_EXCEEDED = "CONTEXT_LENGTH_EXCEEDED",
  CONTENT_FILTERED = "CONTENT_FILTERED",
  
  // 系统错误
  TIMEOUT = "TIMEOUT",
  INTERNAL_ERROR = "INTERNAL_ERROR",
  SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE",
  WEBSOCKET_ERROR = "WEBSOCKET_ERROR",
}

/**
 * 响应错误
 */
export interface ResponseError {
  code: ErrorCode | string;
  message: string;
  details?: string;
}

/**
 * 响应数据
 */
export interface ResponseData {
  content?: string;
  error?: ResponseError;
  choices?: Array<{
    index: number;
    delta: {
      content?: string;
      role?: string;
    };
    finish_reason?: string | null;
  }>;
}

// ============================================================================
// 错误码映射工具
// ============================================================================

/**
 * 从错误消息中推断错误码
 */
export function inferErrorCode(errorText: string): ErrorCode {
  const text = errorText.toLowerCase();
  
  if (text.includes("rate limit") || text.includes("429") || text.includes("too many requests")) {
    return ErrorCode.RATE_LIMITED;
  }
  if (text.includes("quota") || text.includes("insufficient") || text.includes("balance") || text.includes("credit")) {
    return ErrorCode.INSUFFICIENT_QUOTA;
  }
  if (text.includes("overload") || text.includes("capacity") || text.includes("temporarily unavailable")) {
    return ErrorCode.MODEL_OVERLOADED;
  }
  if (text.includes("model not found") || text.includes("does not exist")) {
    return ErrorCode.MODEL_NOT_FOUND;
  }
  if (text.includes("context length") || text.includes("token limit") || text.includes("max tokens")) {
    return ErrorCode.CONTEXT_LENGTH_EXCEEDED;
  }
  if (text.includes("content filtered") || text.includes("content policy") || text.includes("safety")) {
    return ErrorCode.CONTENT_FILTERED;
  }
  if (text.includes("timeout") || text.includes("timed out")) {
    return ErrorCode.TIMEOUT;
  }
  if (text.includes("service unavailable") || text.includes("503")) {
    return ErrorCode.SERVICE_UNAVAILABLE;
  }
  if (text.includes("invalid") || text.includes("bad request")) {
    return ErrorCode.INVALID_REQUEST;
  }
  
  return ErrorCode.INTERNAL_ERROR;
}