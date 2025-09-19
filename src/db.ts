import { Pool, PoolClient } from "pg";
import dotenv from "dotenv";

dotenv.config();

function resolveConnectionString(): string {
  const url = process.env.DATABASE_URL || process.env.PG_URL;
  if (url) return url;

  const host = process.env.PGHOST || "localhost";
  const port = process.env.PGPORT || "5432";
  const user = process.env.PGUSER || "postgres";
  const pass = encodeURIComponent(process.env.PGPASSWORD || "");
  const db = process.env.PGDATABASE || "postgres";
  return `postgres://${user}:${pass}@${host}:${port}/${db}`;
}

const connectionString = resolveConnectionString();
const needsSsl =
  process.env.PGSSL === "require" ||
  /neon|supabase|render|heroku/i.test(connectionString);

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
