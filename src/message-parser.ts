import type { AgentHubIncomingMessage, AgentHubWsMessage } from "./interface.js";

/**
 * 解析来自 Go 后端的消息
 * 支持 OpenAI 兼容的请求格式
 */
export function parseIncomingMessage(rawData: string): AgentHubIncomingMessage | null {
  try {
    const wsMsg: AgentHubWsMessage = JSON.parse(rawData);
    
    // 心跳包过滤
    if (wsMsg.action === "ping" || wsMsg.action === "pong") {
      return null;
    }

    if (wsMsg.action === "chat") {
      const openAIReq = wsMsg.data;
      if (!openAIReq || !openAIReq.messages || !Array.isArray(openAIReq.messages)) {
        return null;
      }

      // 获取最后一条用户消息
      const lastUserMsg = [...openAIReq.messages].reverse().find((m: any) => m.role === "user");
      if (!lastUserMsg) return null;

      // 提取用户 ID 和会话 ID
      const userId = openAIReq.user || lastUserMsg.name || `user-${wsMsg.req_id}`;
      const chatId = openAIReq.conversation_id || userId;

      return {
        type: "message",
        msgId: wsMsg.req_id,
        reqId: wsMsg.req_id,
        chatId: chatId,
        userId: userId,
        text: lastUserMsg.content || "",
      };
    }

    // 兼容旧格式
    const data = wsMsg as any;
    return {
      type: data.type || "message",
      msgId: data.msgId || data.id || `msg-${Date.now()}`,
      chatId: data.chatId || data.userId || "default-chat",
      userId: data.userId || data.chatId || "default-user",
      text: data.text || data.content || "",
      quoteContent: data.quoteContent,
    };
  } catch (err) {
    return null;
  }
}