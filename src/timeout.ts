/**
 * 超时处理工具模块
 */

/**
 * 为 Promise 添加超时保护
 * @param promise 需要包装的 Promise
 * @param ms 超时时间（毫秒）
 * @param message 超时错误消息
 */
export async function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  message: string
): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`Timeout after ${ms}ms: ${message}`));
    }, ms);

    promise
      .then((result) => {
        clearTimeout(timer);
        resolve(result);
      })
      .catch((error) => {
        clearTimeout(timer);
        reject(error);
      });
  });
}