import type { AgentHubIncomingMessage, AgentHubWsMessage, MessageContentItem } from "./interface.js";

export interface ParsedMessageContent {
  textParts: string[];
  imageUrls: string[];
  fileUrls: string[];
  contentItems: MessageContentItem[];
}

function extractTextFromContent(content: unknown): string {
  if (typeof content === "string") {
    return content;
  }
  if (Array.isArray(content)) {
    return content
      .filter((item): item is { type: string; text?: string } => 
        typeof item === "object" && item !== null && item.type === "text"
      )
      .map((item) => item.text || "")
      .join("\n");
  }
  return "";
}

function extractImagesFromContent(content: unknown): { urls: string[]; items: MessageContentItem[] } {
  const urls: string[] = [];
  const items: MessageContentItem[] = [];

  if (Array.isArray(content)) {
    for (const item of content) {
      if (typeof item !== "object" || item === null) continue;
      
      if (item.type === "image_url" && item.image_url?.url) {
        urls.push(item.image_url.url);
        items.push({
          type: "image",
          image: { url: item.image_url.url },
        });
      } else if (item.type === "image" && (item.url || item.base64)) {
        if (item.url) urls.push(item.url);
        items.push({
          type: "image",
          image: {
            url: item.url,
            base64: item.base64,
            mimeType: item.mimeType,
          },
        });
      }
    }
  }

  return { urls, items };
}

function extractFilesFromContent(content: unknown): { urls: string[]; items: MessageContentItem[] } {
  const urls: string[] = [];
  const items: MessageContentItem[] = [];

  if (Array.isArray(content)) {
    for (const item of content) {
      if (typeof item !== "object" || item === null) continue;
      
      if (item.type === "file" && (item.url || item.base64)) {
        if (item.url) urls.push(item.url);
        items.push({
          type: "file",
          file: {
            url: item.url,
            base64: item.base64,
            filename: item.filename,
            mimeType: item.mimeType,
          },
        });
      }
    }
  }

  return { urls, items };
}

export function parseIncomingMessage(rawData: string): AgentHubIncomingMessage | null {
  try {
    const wsMsg: AgentHubWsMessage = JSON.parse(rawData);
    
    if (wsMsg.action === "ping" || wsMsg.action === "pong") {
      return null;
    }

    if (wsMsg.action === "chat") {
      const openAIReq = wsMsg.data;
      if (!openAIReq || !openAIReq.messages || !Array.isArray(openAIReq.messages)) {
        return null;
      }

      const lastUserMsg = [...openAIReq.messages].reverse().find((m: any) => m.role === "user");
      if (!lastUserMsg) return null;

      const userId = openAIReq.user || lastUserMsg.name || `user-${wsMsg.req_id}`;
      const chatId = openAIReq.conversation_id || userId;

      const text = extractTextFromContent(lastUserMsg.content);
      const { urls: imageUrls, items: imageItems } = extractImagesFromContent(lastUserMsg.content);
      const { urls: fileUrls, items: fileItems } = extractFilesFromContent(lastUserMsg.content);
      const contentItems: MessageContentItem[] = [...imageItems, ...fileItems];

      return {
        type: "message",
        msgId: wsMsg.req_id,
        reqId: wsMsg.req_id,
        chatId: chatId,
        userId: userId,
        text,
        imageUrls: imageUrls.length > 0 ? imageUrls : undefined,
        fileUrls: fileUrls.length > 0 ? fileUrls : undefined,
        contentItems: contentItems.length > 0 ? contentItems : undefined,
      };
    }

    const data = wsMsg as any;
    const imageUrls = data.imageUrls || (data.images ? data.images.map((img: any) => img.url || img).filter(Boolean) : []);
    const fileUrls = data.fileUrls || (data.files ? data.files.map((f: any) => f.url || f).filter(Boolean) : []);
    
    return {
      type: data.type || "message",
      msgId: data.msgId || data.id || `msg-${Date.now()}`,
      chatId: data.chatId || data.userId || "default-chat",
      userId: data.userId || data.chatId || "default-user",
      text: data.text || data.content || "",
      imageUrls: imageUrls.length > 0 ? imageUrls : undefined,
      fileUrls: fileUrls.length > 0 ? fileUrls : undefined,
      quoteContent: data.quoteContent,
    };
  } catch (err) {
    return null;
  }
}

export function parseMessageContent(msg: AgentHubIncomingMessage): ParsedMessageContent {
  const textParts: string[] = [];
  const imageUrls: string[] = [];
  const fileUrls: string[] = [];
  const contentItems: MessageContentItem[] = [];

  if (msg.text?.trim()) {
    textParts.push(msg.text.trim());
  }

  if (msg.imageUrls?.length) {
    imageUrls.push(...msg.imageUrls);
    for (const url of msg.imageUrls) {
      contentItems.push({ type: "image", image: { url } });
    }
  }

  if (msg.fileUrls?.length) {
    fileUrls.push(...msg.fileUrls);
    for (const url of msg.fileUrls) {
      contentItems.push({ type: "file", file: { url } });
    }
  }

  if (msg.contentItems?.length) {
    for (const item of msg.contentItems) {
      if (item.type === "image" && item.image?.url && !imageUrls.includes(item.image.url)) {
        imageUrls.push(item.image.url);
      }
      if (item.type === "file" && item.file?.url && !fileUrls.includes(item.file.url)) {
        fileUrls.push(item.file.url);
      }
      contentItems.push(item);
    }
  }

  return { textParts, imageUrls, fileUrls, contentItems };
}