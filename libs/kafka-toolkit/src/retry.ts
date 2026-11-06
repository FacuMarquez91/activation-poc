
export type RetryOptions = {
  onRetry?: (
    error: unknown,
    retryNumber: number,
    delayMs: number,
  ) => void;
};

export const RETRY_DELAYS_MS = [
  1000,
  2000,
  4000,
] as const;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export async function withRetry<T>(
  operation: () => Promise<T>,
  options: RetryOptions = {},
): Promise<T> {
  // Primer intento inmediato.
  // Si falla, se realizan tres reintentos.
  for (
    let attempt = 0;
    attempt <= RETRY_DELAYS_MS.length;
    attempt++
  ) {
    try {
      return await operation();
    } catch (error) {
      const delayMs = RETRY_DELAYS_MS[attempt];

      // Se agotaron los reintentos.
      // El error se propaga al consumidor,
      // que luego podra enviarlo a una DLQ.
      if (delayMs === undefined) {
        throw error;
      }

      options.onRetry?.(
        error,
        attempt + 1,
        delayMs,
      );

      await sleep(delayMs);
    }
  }

  throw new Error(
    'No fue posible completar la operacion',
  );
}
