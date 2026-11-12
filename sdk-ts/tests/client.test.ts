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
});
