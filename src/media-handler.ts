import type { OpenClawConfig, RuntimeEnv } from "openclaw/plugin-sdk";
import type { WebSocket } from "ws";
import { getRuntime } from "./runtime.js";
import { IMAGE_DOWNLOAD_TIMEOUT_MS, FILE_DOWNLOAD_TIMEOUT_MS, DEFAULT_MEDIA_MAX_MB } from "./const.js";
import { withTimeout } from "./timeout.js";
import type { ResolvedAccount } from "./utils.js";

async function detectContentType(data: Buffer): Promise<string> {
  const header = data.slice(0, 12);
  
  if (header[0] === 0xff && header[1] === 0xd8 && header[2] === 0xff) {
    return "image/jpeg";
  }
  if (header[0] === 0x89 && header[1] === 0x50 && header[2] === 0x4e && header[3] === 0x47) {
    return "image/png";
  }
  if (header[0] === 0x47 && header[1] === 0x49 && header[2] === 0x46) {
    return "image/gif";
  }
  if (header[0] === 0x52 && header[1] === 0x49 && header[2] === 0x46 && header[3] === 0x46) {
    if (header[8] === 0x57 && header[9] === 0x45 && header[10] === 0x42 && header[11] === 0x50) {
      return "image/webp";
    }
  }
  if (header[4] === 0x66 && header[5] === 0x74 && header[6] === 0x79 && header[7] === 0x70) {
    const ftyp = header.slice(8, 12).toString("ascii");
    if (ftyp.startsWith("heic") || ftyp.startsWith("heix")) return "image/heic";
    if (ftyp.startsWith("avif")) return "image/avif";
    return "video/mp4";
  }
  if (header[0] === 0x25 && header[1] === 0x50 && header[2] === 0x44 && header[3] === 0x46) {
    return "application/pdf";
  }
  if (header.slice(0, 4).toString("ascii") === "PK\x03\x04") {
    return "application/zip";
  }
  
  return "application/octet-stream";
}

export async function downloadAndSaveImages(params: {
  imageUrls: string[];
  account: ResolvedAccount;
  config: OpenClawConfig;
  runtime: RuntimeEnv;
  wsClient: WebSocket;
}): Promise<Array<{ path: string; contentType?: string }>> {
  const { imageUrls, config, runtime } = params;
  const core = getRuntime();
  const mediaList: Array<{ path: string; contentType?: string }> = [];

  for (const imageUrl of imageUrls) {
    try {
      runtime.log?.(`[53aihub] Downloading image from: ${imageUrl}`);
      const mediaMaxMb = config.agents?.defaults?.mediaMaxMb ?? DEFAULT_MEDIA_MAX_MB;
      const maxBytes = mediaMaxMb * 1024 * 1024;

      const fetched = await withTimeout(
        core.channel.media.fetchRemoteMedia({ url: imageUrl }),
        IMAGE_DOWNLOAD_TIMEOUT_MS,
        `Image download timed out: ${imageUrl}`
      ) as { buffer: Buffer; contentType?: string };

      let imageBuffer = fetched.buffer;
      let imageContentType = fetched.contentType ?? await detectContentType(imageBuffer);

      runtime.log?.(`[53aihub] Image fetched: contentType=${imageContentType}, size=${imageBuffer.length}`);

      const saved = await core.channel.media.saveMediaBuffer(
        imageBuffer,
        imageContentType,
        "inbound",
        maxBytes
      );
      mediaList.push({ path: saved.path, contentType: saved.contentType });
      runtime.log?.(`[53aihub] Image saved to ${saved.path}`);
    } catch (err) {
      runtime.error?.(`[53aihub] Failed to download image: ${String(err)}`);
    }
  }

  return mediaList;
}

export async function downloadAndSaveFiles(params: {
  fileUrls: string[];
  account: ResolvedAccount;
  config: OpenClawConfig;
  runtime: RuntimeEnv;
  wsClient: WebSocket;
}): Promise<Array<{ path: string; contentType?: string }>> {
  const { fileUrls, config, runtime } = params;
  const core = getRuntime();
  const mediaList: Array<{ path: string; contentType?: string }> = [];

  for (const fileUrl of fileUrls) {
    try {
      runtime.log?.(`[53aihub] Downloading file from: ${fileUrl}`);
      const mediaMaxMb = config.agents?.defaults?.mediaMaxMb ?? DEFAULT_MEDIA_MAX_MB;
      const maxBytes = mediaMaxMb * 1024 * 1024;

      const fetched = await withTimeout(
        core.channel.media.fetchRemoteMedia({ url: fileUrl }),
        FILE_DOWNLOAD_TIMEOUT_MS,
        `File download timed out: ${fileUrl}`
      ) as { buffer: Buffer; contentType?: string };

      let fileBuffer = fetched.buffer;
      let fileContentType = fetched.contentType ?? await detectContentType(fileBuffer);

      runtime.log?.(`[53aihub] File fetched: contentType=${fileContentType}, size=${fileBuffer.length}`);

      const saved = await core.channel.media.saveMediaBuffer(
        fileBuffer,
        fileContentType,
        "inbound",
        maxBytes
      );
      mediaList.push({ path: saved.path, contentType: saved.contentType });
      runtime.log?.(`[53aihub] File saved to ${saved.path}`);
    } catch (err) {
      runtime.error?.(`[53aihub] Failed to download file: ${String(err)}`);
    }
  }

  return mediaList;
}