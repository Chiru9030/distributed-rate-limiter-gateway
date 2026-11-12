export interface GatewayErrorPayload {
  type: string;
  code: string;
  message: string;
  param?: string;
  request_id?: string;
  trace_id?: string;
}

export class GatewayApiError extends Error {
  public readonly status: number;
  public readonly type: string;
  public readonly code: string;
  public readonly requestId?: string;
  public readonly traceId?: string;

  constructor(status: number, payload: GatewayErrorPayload) {
    super(payload.message);
    this.name = 'GatewayApiError';
    this.status = status;
    this.type = payload.type;
    this.code = payload.code;
    this.requestId = payload.request_id;
    this.traceId = payload.trace_id;
  }
}

export interface RateLimitInfo {
  limit?: number;
  remaining?: number;
  resetAt?: number;
}

export interface GatewayResponse<T> {
  data: T;
  status: number;
  requestId?: string;
  rateLimit: RateLimitInfo;
}

export interface GatewayClientOptions {
  baseUrl: string;
  apiKey: string;
  maxRetries?: number;
  baseRetryDelayMs?: number;
  timeoutMs?: number;
  fetchFn?: typeof fetch;
  sleepFn?: (ms: number) => Promise<void>;
}

export interface RequestOptions {
  body?: unknown;
  headers?: Record<string, string>;
  idempotencyKey?: string;
  timeoutMs?: number;
}

export class GatewayClient {
  private readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly maxRetries: number;
  private readonly baseRetryDelayMs: number;
  private readonly timeoutMs: number;
  private readonly fetchFn: typeof fetch;
  private readonly sleepFn: (ms: number) => Promise<void>;

  constructor(options: GatewayClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, '');
    this.apiKey = options.apiKey;
    this.maxRetries = options.maxRetries ?? 3;
    this.baseRetryDelayMs = options.baseRetryDelayMs ?? 200;
    this.timeoutMs = options.timeoutMs ?? 5000;
    this.fetchFn = options.fetchFn ?? fetch;
    this.sleepFn = options.sleepFn ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
  }

  get<T>(path: string, options?: Omit<RequestOptions, 'body'>): Promise<GatewayResponse<T>> {
    return this.request<T>('GET', path, options);
  }

  post<T>(path: string, body?: unknown, options?: Omit<RequestOptions, 'body'>): Promise<GatewayResponse<T>> {
    return this.request<T>('POST', path, { ...options, body });
  }

  put<T>(path: string, body?: unknown, options?: Omit<RequestOptions, 'body'>): Promise<GatewayResponse<T>> {
    return this.request<T>('PUT', path, { ...options, body });
  }

  delete<T>(path: string, options?: Omit<RequestOptions, 'body'>): Promise<GatewayResponse<T>> {
    return this.request<T>('DELETE', path, options);
  }

  async request<T>(
    method: string,
    path: string,
    options: RequestOptions = {},
  ): Promise<GatewayResponse<T>> {
    const url = `${this.baseUrl}${path.startsWith('/') ? path : `/${path}`}`;
    const isMutating = method === 'POST' || method === 'PUT' || method === 'PATCH';
    const idempotencyKey =
      options.idempotencyKey ?? (isMutating ? crypto.randomUUID() : undefined);

    let attempt = 0;
    while (true) {
      attempt++;
      const headers: Record<string, string> = {
        Authorization: `Bearer ${this.apiKey}`,
        'X-Request-ID': `sdk_${crypto.randomUUID().slice(0, 12)}`,
        ...(options.body !== undefined ? { 'Content-Type': 'application/json' } : {}),
        ...(idempotencyKey ? { 'Idempotency-Key': idempotencyKey } : {}),
        ...options.headers,
      };

      const res = await this.fetchFn(url, {
        method,
        headers,
        body: options.body !== undefined ? JSON.stringify(options.body) : undefined,
        signal: AbortSignal.timeout(options.timeoutMs ?? this.timeoutMs),
      });

      const rateLimit: RateLimitInfo = {
        limit: parseOptionalInt(res.headers.get('x-ratelimit-limit')),
        remaining: parseOptionalInt(res.headers.get('x-ratelimit-remaining')),
        resetAt: parseOptionalInt(res.headers.get('x-ratelimit-reset')),
      };

      const requestId = res.headers.get('x-request-id') ?? undefined;

      if (res.ok) {
        const text = await res.text();
        const data = (text ? JSON.parse(text) : undefined) as T;
        return { data, status: res.status, requestId, rateLimit };
      }

      const retryable = res.status === 429 || res.status >= 500;
      if (retryable && attempt <= this.maxRetries) {
        const retryAfterSec = parseOptionalInt(res.headers.get('retry-after'));
        const delayMs = retryAfterSec
          ? retryAfterSec * 1000
          : this.baseRetryDelayMs * Math.pow(2, attempt - 1);
        await this.sleepFn(delayMs);
        continue;
      }

      const raw = await res.text();
      try {
        const parsed = JSON.parse(raw) as { error?: GatewayErrorPayload };
        if (parsed.error) {
          throw new GatewayApiError(res.status, parsed.error);
        }
      } catch (err) {
        if (err instanceof GatewayApiError) throw err;
      }

      throw new GatewayApiError(res.status, {
        type: 'api_error',
        code: 'unexpected_response',
        message: raw || `HTTP ${res.status}`,
        request_id: requestId,
      });
    }
  }
}

function parseOptionalInt(val: string | null): number | undefined {
  if (!val) return undefined;
  const n = Number(val);
  return Number.isFinite(n) ? n : undefined;
}
