import type { SessionMessage } from "./models";

const HUB_SESSION_TITLE_PREFIX = "53AI Hub-";
const CONTROL_CENTER_SESSION_TITLE = "Claw Control Center";

export function isHubTitle(title: string | undefined): title is string {
  return typeof title === "string" && title.trim().startsWith(HUB_SESSION_TITLE_PREFIX);
}

export function isControlCenterTitle(title: string): boolean {
  return title.trim() === CONTROL_CENTER_SESSION_TITLE;
}

export function preserveExistingHubTitle(existingTitle: string | undefined, incomingTitle: string): string {
  if (isHubTitle(existingTitle) && isControlCenterTitle(incomingTitle)) {
    return existingTitle;
  }
  return incomingTitle;
}

export function readHubMessageMetadata(message: SessionMessage): Record<string, unknown> {
  return {
    ...toRecord(message.payload),
    ...toRecord(message.data),
    ...toRecord(message.metadata),
    ...toRecord((message as Record<string, unknown>).__openclaw)
  };
}

export function readHubClientMessageId(message: SessionMessage): string {
  const metadata = readHubMessageMetadata(message);
  return stringOr(
    metadata.openclaw_client_message_id,
    metadata.client_message_id,
    metadata.clientMessageId
  );
}

export function isHubUserMessagePatch(message: SessionMessage): boolean {
  if (message.role !== "user") {
    return false;
  }
  const metadata = readHubMessageMetadata(message);
  return Boolean(
    stringOr(metadata.openclaw_client_message_id) ||
      (Array.isArray(metadata.openclaw_input_files) && metadata.openclaw_input_files.length > 0) ||
      Object.keys(toRecord(metadata.openclaw_skill)).length > 0
  );
}

export function findHubUserMessagePatch(
  message: SessionMessage,
  patches: SessionMessage[],
  usedPatchIndexes: Set<number>
): number {
  const clientMessageId = readHubClientMessageId(message);
  if (clientMessageId) {
    const byClientId = patches.findIndex(
      (patch, index) => !usedPatchIndexes.has(index) && readHubClientMessageId(patch) === clientMessageId
    );
    if (byClientId >= 0) {
      return byClientId;
    }
  }

  const normalizedContent = normalizeHubUserMessageContentForMatch(message.content);
  if (!normalizedContent) {
    return -1;
  }
  const messageTime = Date.parse(message.createdAt || "");
  let bestIndex = -1;
  let bestDistance = Number.MAX_SAFE_INTEGER;
  for (let index = 0; index < patches.length; index += 1) {
    if (usedPatchIndexes.has(index)) {
      continue;
    }
    const patch = patches[index]!;
    if (normalizeHubUserMessageContentForMatch(patch.content) !== normalizedContent) {
      continue;
    }
    const patchTime = Date.parse(patch.createdAt || "");
    const distance = Number.isFinite(messageTime) && Number.isFinite(patchTime)
      ? Math.abs(messageTime - patchTime)
      : 0;
    if (distance > 10 * 60 * 1000) {
      continue;
    }
    if (distance < bestDistance) {
      bestDistance = distance;
      bestIndex = index;
    }
  }
  return bestIndex;
}

export function applyHubUserMessagePatch(message: SessionMessage, patch: SessionMessage): SessionMessage {
  const cleanPatchContent = stripHubRuntimeContextFromContent(patch.content);
  return {
    ...message,
    content: cleanPatchContent || stripHubRuntimeContextFromContent(message.content),
    metadata: {
      ...toRecord(message.metadata),
      ...readHubMessageMetadata(patch)
    }
  };
}

export function mergePreservedHubUserMetadata(incoming: SessionMessage[], existing: SessionMessage[]): SessionMessage[] {
  const patches = existing.filter(isHubUserMessagePatch);
  if (!patches.length) {
    return incoming;
  }
  const usedPatchIndexes = new Set<number>();
  return incoming.map((message) => {
    if (message.role !== "user") {
      return message;
    }
    const patchIndex = findHubUserMessagePatch(message, patches, usedPatchIndexes);
    if (patchIndex < 0) {
      return message;
    }
    usedPatchIndexes.add(patchIndex);
    return applyHubUserMessagePatch(message, patches[patchIndex]!);
  });
}

export const mergeHubUserMessageMetadata = mergePreservedHubUserMetadata;

export function mergeHubLocalAssistantMessages(messages: SessionMessage[], localMessages: SessionMessage[]): SessionMessage[] {
  const localAssistant = localMessages.filter((m) => m.role === "assistant" && m.content?.trim());
  if (!localAssistant.length) {
    return messages;
  }
  const gatewayContents = new Set<string>();
  for (const m of messages) {
    if (m.role === "assistant") {
      const norm = m.content?.replace(/\s+/g, " ").trim();
      if (norm) gatewayContents.add(norm);
    }
  }
  const missing = localAssistant.filter((m) => {
    const norm = m.content?.replace(/\s+/g, " ").trim();
    return !norm || !gatewayContents.has(norm);
  });
  if (!missing.length) {
    return messages;
  }
  return [...messages, ...missing];
}

export function normalizeHubUserMessageContentForMatch(content: string): string {
  return stripHubRuntimeContextFromContent(content).replace(/\s+/g, " ").trim();
}

export function stripHubRuntimeContextFromContent(content: string): string {
  const withoutSentinel = String(content || "").replace(
    /<53aihub-openclaw-runtime-context>[\s\S]*?<\/53aihub-openclaw-runtime-context>/gi,
    ""
  );
  const lines = withoutSentinel.split(/\r?\n/);
  const kept: string[] = [];
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] || "";
    const lower = line.trim().toLowerCase();
    if (lower === "local input files:" || lower === "remote input files:" || lower === "attached files:" || lower === "files:") {
      while (index + 1 < lines.length && (lines[index + 1] || "").trim()) {
        index += 1;
      }
      continue;
    }
    if (lower.startsWith("selected skill:")) {
      continue;
    }
    if (lower.startsWith("use the installed local skill with this name")) {
      continue;
    }
    if (/^@(?:\/|~\/)/.test(line.trim())) {
      continue;
    }
    kept.push(line);
  }
  return kept.join("\n").trim();
}

export function stringOr(...values: unknown[]): string {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return "";
}

export function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

export function dedupeMessages(messages: SessionMessage[]): SessionMessage[] {
  return Array.from(new Map(messages.map((message) => [message.id, message])).values()).sort((left, right) =>
    left.createdAt.localeCompare(right.createdAt)
  );
}
