import type { RuntimeEnv } from "openclaw/plugin-sdk";
import type { WebSocket } from "ws";
import type { Hub53AIWsMessage, ResponseError, Hub53AIOutgoingMessage } from "./interface.js";
import { ErrorCode } from "./interface.js";

interface SendReplyParams {
  wsClient: WebSocket;
  text: string;
  toChatId: string;
  replyToMsgId?: string;
  runtime: RuntimeEnv;
  finish: boolean;
  streamId: string;
  isError?: boolean;
  errorCode?: ErrorCode | string;
  errorDetails?: string;
}

export async function sendReply(params: SendReplyParams): Promise<void> {
  const { wsClient, text, toChatId, replyToMsgId, runtime, finish, streamId, isError, errorCode, errorDetails } = params;

  const reqId = replyToMsgId || streamId;

  runtime.log?.(`[53aihub] sendReply START: reqId=${reqId}, finish=${finish}, isError=${isError}, textLen=${text?.length || 0}, wsReadyState=${wsClient.readyState}`);

  if (wsClient.readyState !== 1) {
    runtime.error?.(`[53aihub] WebSocket is not open (readyState=${wsClient.readyState}). Cannot send message to ${toChatId}`);
    return;
  }

  if (isError) {
    runtime.error?.(`[53aihub] sendReply ERROR: reqId=${reqId}, code=${errorCode}, text=${text?.substring(0, 100)}`);
    
    const errorInfo: ResponseError = {
      code: errorCode || ErrorCode.INTERNAL_ERROR,
      message: text || "Unknown error",
      details: errorDetails,
    };

    const errorChunk = {
      id: reqId,
      object: "chat.completion.chunk",
      created: Math.floor(Date.now() / 1000),
      model: "openclaw-agent",
      choices: [
        {
          index: 0,
          delta: {
            content: text,
            role: "assistant",
          },
          finish_reason: "error",
        },
      ],
      error: errorInfo,
    };

    const errMsg: Hub53AIWsMessage = {
      req_id: reqId,
      action: "chat",
      status: "error",
      data: errorChunk,
    };

    const jsonStr = JSON.stringify(errMsg);
    runtime.log?.(`[53aihub] sendReply ERROR SENDING: reqId=${reqId}, payloadLen=${jsonStr.length}`);
    wsClient.send(jsonStr);
    return;
  }

  const chunk = {
    id: reqId,
    object: "chat.completion.chunk",
    created: Math.floor(Date.now() / 1000),
    model: "openclaw-agent",
    choices: [
      {
        index: 0,
        delta: {
          content: text,
          role: "assistant",
        },
        finish_reason: finish ? "stop" : null,
      },
    ],
  };

  const payload: Hub53AIWsMessage = {
    req_id: reqId,
    action: "chat",
    status: finish ? "done" : "streaming",
    data: chunk,
  };

  const jsonStr = JSON.stringify(payload);
  runtime.log?.(`[53aihub] sendReply SENDING: reqId=${reqId}, status=${payload.status}, textLen=${text?.length || 0}, payloadLen=${jsonStr.length}, textPreview=${text?.substring(0, 50) || "(empty)"}`);

  try {
    wsClient.send(jsonStr);
    runtime.log?.(`[53aihub] sendReply SENT: reqId=${reqId}, status=${payload.status}`);
  } catch (error) {
    runtime.error?.(`[53aihub] sendReply FAILED: reqId=${reqId}, error=${String(error)}`);
    throw error;
  }
}

export async function sendDirectMessage(wsClient: WebSocket, to: string, content: string, runtime?: RuntimeEnv): Promise<void> {
  if (wsClient.readyState !== 1) {
    throw new Error(`[53aihub] WebSocket not connected`);
  }

  const payload: Hub53AIWsMessage = {
    req_id: `msg-${Date.now()}`,
    action: "message",
    status: "final",
    data: {
      toChatId: to,
      text: content,
    },
  };

  runtime?.log?.(`[53aihub] sendDirectMessage: to=${to}, contentLen=${content.length}`);
  wsClient.send(JSON.stringify(payload));
}

export async function sendMediaMessage(
  wsClient: WebSocket,
  to: string,
  media: {
    type: "image" | "file";
    url?: string;
    base64?: string;
    mimeType?: string;
    filename?: string;
  },
  text?: string,
  runtime?: RuntimeEnv
): Promise<void> {
  if (wsClient.readyState !== 1) {
    throw new Error(`[53aihub] WebSocket not connected`);
  }

  const payload: Hub53AIWsMessage = {
    req_id: `msg-${Date.now()}`,
    action: "message",
    status: "final",
    data: {
      toChatId: to,
      text: text || "",
      media: {
        type: media.type,
        url: media.url,
        base64: media.base64,
        mimeType: media.mimeType,
        filename: media.filename,
      },
    },
  };

  runtime?.log?.(`[53aihub] sendMediaMessage: to=${to}, type=${media.type}, url=${media.url ? "provided" : "none"}`);
  wsClient.send(JSON.stringify(payload));
}

export async function sendThinkingMessage(
  wsClient: WebSocket,
  text: string,
  msgId: string,
  streamId: string,
  runtime?: RuntimeEnv
): Promise<void> {
  const wsState = wsClient.readyState;
  runtime?.log?.(`[53aihub] sendThinkingMessage CALLED: msgId=${msgId}, streamId=${streamId}, text=${text}, wsReadyState=${wsState}`);

  if (wsState !== 1) {
    runtime?.error?.(`[53aihub] sendThinkingMessage SKIPPED: WebSocket not ready (state=${wsState})`);
    return;
  }

  // 使用 OpenAI 兼容格式，确保 Go 后端能正确解析
  // 关键：req_id 必须使用原始消息的 msgId，这样 Go 后端才能关联请求和响应
  const chunk = {
    id: streamId,
    object: "chat.completion.chunk",
    created: Math.floor(Date.now() / 1000),
    model: "openclaw-agent",
    choices: [
      {
        index: 0,
        delta: {
          content: text,
          role: "assistant",
        },
        finish_reason: null,
      },
    ],
  };

  const payload: Hub53AIWsMessage = {
    req_id: msgId,
    action: "chat",
    status: "thinking",
    data: chunk,
  };

  const jsonStr = JSON.stringify(payload);
  runtime?.log?.(`[53aihub] sendThinkingMessage SENDING: ${jsonStr}`);
  
  try {
    wsClient.send(jsonStr);
    runtime?.log?.(`[53aihub] sendThinkingMessage SENT SUCCESS: msgId=${msgId}`);
  } catch (err) {
    runtime?.error?.(`[53aihub] sendThinkingMessage SEND FAILED: ${String(err)}`);
  }
}