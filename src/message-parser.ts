import type { Hub53AIIncomingMessage, Hub53AIWsMessage, MessageContentItem, OpenAIChatRequest, MessageData } from "./interface.js";

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

      const itemRecord = item as Record<string, unknown>;

      if (itemRecord.type === "image_url" &&
        typeof itemRecord.image_url === "object" &&
        itemRecord.image_url !== null &&
        "url" in itemRecord.image_url) {
        const url = String((itemRecord.image_url as Record<string, unknown>).url);
        urls.push(url);
        items.push({
          type: "image",
          image: { url },
        });
      } else if (itemRecord.type === "image" && (itemRecord.url || itemRecord.base64)) {
        if (typeof itemRecord.url === "string") urls.push(itemRecord.url);
        items.push({
          type: "image",
          image: {
            url: typeof itemRecord.url === "string" ? itemRecord.url : undefined,
            base64: typeof itemRecord.base64 === "string" ? itemRecord.base64 : undefined,
            mimeType: typeof itemRecord.mimeType === "string" ? itemRecord.mimeType : undefined,
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

      const itemRecord = item as Record<string, unknown>;

      if (itemRecord.type === "file" && (itemRecord.url || itemRecord.base64)) {
        if (typeof itemRecord.url === "string") urls.push(itemRecord.url);
        items.push({
          type: "file",
          file: {
            url: typeof itemRecord.url === "string" ? itemRecord.url : undefined,
            base64: typeof itemRecord.base64 === "string" ? itemRecord.base64 : undefined,
            filename: typeof itemRecord.filename === "string" ? itemRecord.filename : undefined,
            mimeType: typeof itemRecord.mimeType === "string" ? itemRecord.mimeType : undefined,
          },
        });
      }
    }
  }

  return { urls, items };
}

export function parseIncomingMessage(rawJson: string): Hub53AIIncomingMessage | null {
  try {
    const wsMsg = JSON.parse(rawJson) as Hub53AIWsMessage;

    if (wsMsg.action === "ping" || wsMsg.action === "pong") {
      return null;
    }

    if (wsMsg.action === "chat") {
      const openAIReq = wsMsg.data as OpenAIChatRequest;
      if (!openAIReq || !openAIReq.messages || !Array.isArray(openAIReq.messages)) {
        return null;
      }

      const lastUserMsg = [...openAIReq.messages].reverse().find((m) => m.role === "user");
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

    // 处理非标准格式的消息 (action === "message")
    const data = wsMsg.data as MessageData;
    const dataRecord = data as Record<string, unknown>;
    const rawImages = dataRecord.images;
    const rawFiles = dataRecord.files;

    const imageUrls: string[] = data.imageUrls ||
      (Array.isArray(rawImages) ? rawImages.map((img: unknown) => {
        if (typeof img === "string") return img;
        if (typeof img === "object" && img !== null && "url" in img) return String((img as Record<string, unknown>).url);
        return "";
      }).filter(Boolean) : []);

    const fileUrls: string[] = data.fileUrls ||
      (Array.isArray(rawFiles) ? rawFiles.map((f: unknown) => {
        if (typeof f === "string") return f;
        if (typeof f === "object" && f !== null && "url" in f) return String((f as Record<string, unknown>).url);
        return "";
      }).filter(Boolean) : []);

    return {
      type: (dataRecord.type as string) || "message",
      msgId: (dataRecord.msgId as string) || (dataRecord.id as string) || `msg-${Date.now()}`,
      chatId: (dataRecord.chatId as string) || (dataRecord.userId as string) || "default-chat",
      userId: (dataRecord.userId as string) || (dataRecord.chatId as string) || "default-user",
      text: (dataRecord.text as string) || (dataRecord.content as string) || "",
      imageUrls: imageUrls.length > 0 ? imageUrls : undefined,
      fileUrls: fileUrls.length > 0 ? fileUrls : undefined,
      quoteContent: dataRecord.quoteContent as string | undefined,
    };
  } catch {
    return null;
  }
}

export function parseMessageContent(msg: Hub53AIIncomingMessage): ParsedMessageContent {
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
