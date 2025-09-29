import type { Response } from 'express';

type SSEPayload = {
  topic: string;
  action: string;
  id?: string;
  payload?: unknown;
};

type SSEClient = {
  id: number;
  res: Response;
};

let nextClientId = 1;
const clients = new Set<SSEClient>();

function serialize(event: string, data: unknown): string {
  return `event: ${event}\n` + `data: ${JSON.stringify(data)}\n\n`;
}

export function setupSSEClient(res: Response): () => void {
  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  (res as any).flushHeaders?.();

  const client: SSEClient = { id: nextClientId++, res };
  clients.add(client);

  res.write(serialize('hello', { ok: true, now: Date.now() }));

  const heartbeat = setInterval(() => {
    res.write(`: ping\n\n`);
  }, 25_000);

  return () => {
    clearInterval(heartbeat);
    clients.delete(client);
  };
}

export function sseBroadcast(event: SSEPayload): void {
  for (const client of clients) {
    try {
      client.res.write(
        serialize(event.topic, {
          action: event.action,
          id: event.id ?? null,
          payload: event.payload ?? null,
        })
      );
    } catch {
      clients.delete(client);
    }
  }
}
