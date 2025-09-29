// src/db.ts
import { Pool, PoolClient } from "pg";
import dotenv from "dotenv";

dotenv.config();

function resolveConnectionString(): string {
  // Se qualquer uma das PG* estiver definida, monta o DSN a partir delas
  if (process.env.PGHOST || process.env.PGUSER || process.env.PGPASSWORD || process.env.PGDATABASE || process.env.PGPORT) {
    const host = process.env.PGHOST || "localhost";
    const port = process.env.PGPORT || "5432";
    const user = process.env.PGUSER || "postgres";
    const pass = encodeURIComponent(process.env.PGPASSWORD || "");
    const db   = process.env.PGDATABASE || "postgres";
    return `postgres://${user}:${pass}@${host}:${port}/${db}`;
  }

  // Caso contrário, caia para DATABASE_URL/PG_URL (ex.: produção)
  const url = process.env.DATABASE_URL || process.env.PG_URL;
  if (url) return url;

  // Fallback local
  return "postgres://postgres:@localhost:5432/postgres";
}

const connectionString = resolveConnectionString();

// <- NOVO: considera PGSSLMODE, PGSSL, sslmode=require na URL e provedores comuns
const pgSslEnv = String(process.env.PGSSLMODE || process.env.PGSSL || "").toLowerCase();
const urlForcesSSL =
  /(^|[?&])sslmode=require/i.test(connectionString) ||
  /neon|supabase|render|heroku/i.test(connectionString);

const needsSsl = pgSslEnv === "require" || urlForcesSSL;

export const pool = new Pool({
  connectionString,
  ssl: needsSsl ? { rejectUnauthorized: false } : undefined,
  max: Number.parseInt(process.env.PGPOOL_MAX || "10", 10),
  idleTimeoutMillis: 30_000,
  allowExitOnIdle: false,
});

export async function withTx<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    try { await client.query("ROLLBACK"); } catch {}
    throw err;
  } finally {
    client.release();
  }
}
