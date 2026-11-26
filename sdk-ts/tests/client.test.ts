import { createServer } from 'node:http';
import { describe, expect, it } from 'vitest';
import { GatewayApiError, GatewayClient } from '../src/index.js';

describe('GatewayClient SDK', () => {
  it('retries on 429 honoring Retry-After and preserves Idempotency-Key', async () => {
    const seenKeys: string[] = [];
    let calls = 0;

    const mockFetch: typeof fetch = async (_url, init) => {
      calls++;
      const headers = init?.headers as Record<string, string>;
      seenKeys.push(headers['Idempotency-Key']);
      if (calls === 1) {
        return new Response(
          JSON.stringify({
            error: {
              type: 'rate_limit_error',
              code: 'rate_limit_exceeded',
              message: 'Too many requests',
              request_id: 'req_1',
            },
          }),
          { status: 429, headers: { 'Retry-After': '1', 'X-Request-ID': 'req_1' } },
        );
      }
      return new Response(JSON.stringify({ id: 'txn_123' }), {
        status: 201,
        headers: {
          'X-Request-ID': 'req_2',
          'X-RateLimit-Limit': '100',
          'X-RateLimit-Remaining': '99',
        },
      });
    };

    const slept: number[] = [];
    const client = new GatewayClient({
      baseUrl: 'http://localhost:8080',
      apiKey: 'sk_test_123',
      maxRetries: 2,
      fetchFn: mockFetch,
      sleepFn: async (ms) => {
        slept.push(ms);
      },
    });

    const res = await client.post<{ id: string }>('/v1/transfers', { amount: 500 });
    expect(res.data.id).toBe('txn_123');
    expect(res.rateLimit.remaining).toBe(99);
    expect(slept).toEqual([1000]);
    expect(seenKeys).toHaveLength(2);
    expect(seenKeys[0]).toBe(seenKeys[1]);
  });

  it('throws typed GatewayApiError on 401 unauthorized', async () => {
    const mockFetch: typeof fetch = async () =>
      new Response(
        JSON.stringify({
          error: {
            type: 'authentication_error',
            code: 'invalid_api_key',
            message: 'Missing or invalid API key',
            request_id: 'req_auth_1',
          },
        }),
        { status: 401 },
      );

    const client = new GatewayClient({
      baseUrl: 'http://localhost:8080',
      apiKey: 'bad_key',
      fetchFn: mockFetch,
    });

    await expect(client.get('/v1/charges')).rejects.toMatchObject({
      name: 'GatewayApiError',
      status: 401,
      code: 'invalid_api_key',
      requestId: 'req_auth_1',
    });
    expect(GatewayApiError).toBeDefined();
  });

  it('v2: executes batch requests with bounded concurrency', async () => {
    let active = 0;
    let maxObservedActive = 0;

    const mockFetch: typeof fetch = async (_url, init) => {
      active++;
      if (active > maxObservedActive) maxObservedActive = active;
      await new Promise((r) => setTimeout(r, 10));
      active--;
      return new Response(JSON.stringify({ status: 'ok', body: init?.body }), { status: 200 });
    };

    const client = new GatewayClient({
      baseUrl: 'http://localhost:8080',
      apiKey: 'sk_test_123',
      fetchFn: mockFetch,
    });

    const batchReqs = Array.from({ length: 6 }, (_, i) => ({
      method: 'POST',
      path: `/v1/events/${i}`,
      body: { idx: i },
    }));

    const responses = await client.batch<{ status: string }>(batchReqs, 2);
    expect(responses).toHaveLength(6);
    expect(maxObservedActive).toBeLessThanOrEqual(2);
    expect(responses[0].status).toBe(200);
  });

  it('handles lowercase methods, network/5xx exponential retries, concurrency<=0 clamp, and live HTTP server roundtrip', async () => {
    let attempts = 0;
    let seenIdemKey: string | undefined;
    const sleeps: number[] = [];

    const mockFetch: typeof fetch = async (_url, init) => {
      attempts++;
      const headers = init?.headers as Record<string, string>;
      seenIdemKey = headers['Idempotency-Key'];
      if (attempts === 1) {
        throw new Error('ECONNRESET');
      }
      if (attempts === 2) {
        return new Response('upstream bad', { status: 503 });
      }
      return new Response(JSON.stringify({ ok: true }), { status: 200 });
    };

    const client = new GatewayClient({
      baseUrl: 'http://example.internal',
      apiKey: 'demo_key',
      maxRetries: 3,
      baseRetryDelayMs: 50,
      fetchFn: mockFetch,
      sleepFn: async (ms) => {
        sleeps.push(ms);
      },
    });

    const res = await client.request<{ ok: boolean }>('post', '/v1/orders', { body: { item: 1 } });
    expect(res.data.ok).toBe(true);
    expect(seenIdemKey).toBeTruthy();
    expect(sleeps).toEqual([50, 100]);

    const clamped = await client.batch<{ ok: boolean }>([{ method: 'get', path: '/v1/orders' }], 0);
    expect(clamped).toHaveLength(1);
    expect(clamped[0].status).toBe(200);

    // Real Node HTTP server integration test (zero mocking)
    const server = createServer((req, res) => {
      res.writeHead(200, { 'Content-Type': 'application/json', 'X-RateLimit-Remaining': '9' });
      res.end(JSON.stringify({ auth: req.headers.authorization, method: req.method }));
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const addr = server.address();
    const port = typeof addr === 'object' && addr ? addr.port : 0;

    try {
      const liveClient = new GatewayClient({
        baseUrl: `http://127.0.0.1:${port}`,
        apiKey: 'live_key_123',
      });
      const liveRes = await liveClient.get<{ auth: string; method: string }>('/v1/ping');
      expect(liveRes.data.auth).toBe('Bearer live_key_123');
      expect(liveRes.rateLimit.remaining).toBe(9);
    } finally {
      server.close();
    }
  });
});
