// scripts/restore_from_firestore.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

/** ---------------- Firebase Admin ---------------- */
function initFirebase() {
  // 1) Tente pelas envs
  const hasEnvCreds =
    process.env.FB_PROJECT_ID &&
    process.env.FB_CLIENT_EMAIL &&
    process.env.FB_PRIVATE_KEY;
  if (hasEnvCreds) {
    const projectId = process.env.FB_PROJECT_ID;
    const clientEmail = process.env.FB_CLIENT_EMAIL;
    const privateKey = process.env.FB_PRIVATE_KEY.replace(/\\n/g, '\n');
    if (!admin.apps.length) {
      admin.initializeApp({
        credential: admin.credential.cert({ projectId, clientEmail, privateKey }),
      });
    }
    return admin.firestore();
  }

  // 2) Tente por arquivo (service-account.json OU serviceAccount.json)
  const candidates = [
    path.resolve(process.cwd(), 'service-account.json'),
    path.resolve(process.cwd(), 'serviceAccount.json'),
  ];
  let svc = null;
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      svc = require(p);
      break;
    }
  }
  if (!svc) {
    console.error(
      'ERRO: credenciais do Firebase não encontradas. ' +
        'Defina FB_PROJECT_ID/FB_CLIENT_EMAIL/FB_PRIVATE_KEY no .env ' +
        'OU coloque service-account.json na raiz do projeto.'
    );
    process.exit(1);
  }
  if (!admin.apps.length) {
    admin.initializeApp({
      credential: admin.credential.cert(svc),
    });
  }
  return admin.firestore();
}

/** ---------------- Postgres ---------------- */
function buildPgConnection() {
  const connectionString =
    process.env.DATABASE_URL ||
    process.env.PG_URL ||
    (() => {
      const host = process.env.PGHOST || 'localhost';
      const port = process.env.PGPORT || '5432';
      const user = process.env.PGUSER || 'postgres';
      const pass = encodeURIComponent(process.env.PGPASSWORD || '');
      const db = process.env.PGDATABASE || 'postgres';
      return `postgres://${user}:${pass}@${host}:${port}/${db}`;
    })();

  const needsSsl =
    process.env.PGSSL === 'require' ||
    /supabase|neon|render|heroku/i.test(connectionString);

  return { connectionString, ssl: needsSsl ? { rejectUnauthorized: false } : undefined };
}

/** ---------------- Utils ---------------- */
function normalizeName(s) {
  return String(s || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .trim();
}

function parseArgs() {
  const out = { from: null, to: null, dryRun: false };
  for (const a of process.argv.slice(2)) {
    if (a.startsWith('--from=')) out.from = a.slice(7);
    else if (a.startsWith('--to=')) out.to = a.slice(5);
    else if (a === '--dry-run') out.dryRun = true;
  }
  return out;
}

/** ---------------- Schema helpers ---------------- */
async function ensureSchema(client) {
  // 1) Colunas base
  await client.query(`
    ALTER TABLE public.checklist_submissoes
      ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
      ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now(),
      ADD COLUMN IF NOT EXISTS data_ref date;
  `);

  // 2) Preencher data_ref se nulo
  await client.query(`
    UPDATE public.checklist_submissoes
       SET data_ref = (created_at AT TIME ZONE 'America/Sao_Paulo')::date
     WHERE data_ref IS NULL;
  `);

  // 3) Travar defaults / not null
  await client.query(`
    ALTER TABLE public.checklist_submissoes
      ALTER COLUMN data_ref SET DEFAULT ((now() AT TIME ZONE 'America/Sao_Paulo')::date),
      ALTER COLUMN data_ref SET NOT NULL;
  `);

  // 4) Deduplicar se houver conflitos
  const dups = await client.query(`
    SELECT operador_id, maquina_id, data_ref, COUNT(*) AS c
      FROM public.checklist_submissoes
     GROUP BY 1,2,3
    HAVING COUNT(*) > 1
     ORDER BY c DESC;
  `);

  if (dups.rowCount > 0) {
    console.log(`⚠️  Duplicatas detectadas: ${dups.rowCount} combinações. Deduplicando (mantendo o mais recente)...`);
    await client.query(`
      WITH marcados AS (
        SELECT id,
               ROW_NUMBER() OVER (
                 PARTITION BY operador_id, maquina_id, data_ref
                 ORDER BY created_at DESC, id DESC
               ) AS rn
        FROM public.checklist_submissoes
      )
      DELETE FROM public.checklist_submissoes s
      USING marcados m
      WHERE s.id = m.id
        AND m.rn > 1;
    `);
  }

  // 5) Índice único
  await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS uq_submissao_unica_dia
                      ON public.checklist_submissoes (operador_id, maquina_id, data_ref);`);
}

/** ---------------- Main ---------------- */
(async () => {
  // Conexões
  const fsdb = initFirebase();
  const pgInfo = buildPgConnection();
  const client = new Client(pgInfo);
  await client.connect();
  await client.query('select 1');
  console.log('✅ Conectado ao Postgres.');

  // Garantir schema
  await ensureSchema(client);
  console.log('✅ Schema conferido.');

  // Mapas auxiliares
  const usersByName = new Map();    // nome_normalizado -> { id, email, nome }
  const machinesByName = new Map(); // nome_normalizado -> { id, nome }

  {
    const { rows } = await client.query(
      `SELECT id, nome, lower(email) as email FROM public.usuarios`
    );
    for (const r of rows) usersByName.set(normalizeName(r.nome), { id: r.id, email: r.email, nome: r.nome });
    console.log(`👤 Usuarios carregados: ${rows.length}`);
  }
  {
    const { rows } = await client.query(`SELECT id, nome FROM public.maquinas`);
    for (const r of rows) machinesByName.set(normalizeName(r.nome), { id: r.id, nome: r.nome });
    console.log(`🛠️  Máquinas carregadas: ${rows.length}`);
  }

  // Args / filtros
  const args = parseArgs();
  const dry = args.dryRun;

  // Query Firestore
  let query = fsdb.collection('checklistSubmissions');
  let rangeDesc = 'todas as datas';
  if (args.from) {
    const d = new Date(args.from + 'T00:00:00.000-03:00'); // SP
    query = query.where('dataSubmissao', '>=', d);
    rangeDesc = `de ${args.from} em diante`;
  }
  if (args.to) {
    const d = new Date(args.to + 'T23:59:59.999-03:00'); // SP
    query = query.where('dataSubmissao', '<=', d);
    rangeDesc = args.from ? `de ${args.from} até ${args.to}` : `até ${args.to}`;
  }

  console.log(`🔥 Lendo Firestore (checklistSubmissions) — ${rangeDesc}...`);
  const snap = await query.get();
  console.log(`🔥 Documentos encontrados: ${snap.size}`);

  // Import
  let upserts = 0, skipUser = 0, skipMachine = 0, skipTs = 0, errors = 0;

  for (const doc of snap.docs) {
    const d = doc.data() || {};

    const operadorNome = (d.operadorNome || '').toString();
    const maquinaNome  = (d.maquinaNome  || '').toString();
    const respostas    = d.respostas || {};
    const turno        = (d.turno || '').toString();

    const ts = d.dataSubmissao;
    const createdAt = ts && typeof ts.toDate === 'function' ? ts.toDate() : null;
    if (!createdAt) { skipTs++; continue; }
    const createdAtISO = createdAt.toISOString();

    const user = usersByName.get(normalizeName(operadorNome));
    if (!user) {
      skipUser++;
      console.log('↪️ SKIP sem usuário mapeado ->', operadorNome, '(doc:', doc.id, ')');
      continue;
    }
    const maq  = machinesByName.get(normalizeName(maquinaNome));
    if (!maq) {
      skipMachine++;
      console.log('↪️ SKIP sem máquina mapeada ->', maquinaNome, '(doc:', doc.id, ')');
      continue;
    }

    const sql = `
      INSERT INTO public.checklist_submissoes
        (operador_id, operador_nome, operador_email,
        maquina_id,  maquina_nome,  respostas, turno,
        created_at,  data_ref)
      VALUES
        ($1,$2,$3,$4,$5,$6::jsonb,
        /* turno normalizado: aceita turno1/turno2, 1/2, 1º/2º, vazio etc. */
        CASE
          WHEN lower($7) IN ('turno1','1','1º','1o','1°','primeiro') THEN '1º'
          WHEN lower($7) IN ('turno2','2','2º','2o','2°','segundo')   THEN '2º'
          WHEN coalesce($7,'') = '' THEN
            CASE WHEN (($8::timestamptz AT TIME ZONE 'America/Sao_Paulo')::time) < '14:00' THEN '1º' ELSE '2º' END
          ELSE
            CASE
              WHEN regexp_replace(lower($7),'[^0-9]','','g') = '1' THEN '1º'
              WHEN regexp_replace(lower($7),'[^0-9]','','g') = '2' THEN '2º'
              ELSE CASE WHEN (($8::timestamptz AT TIME ZONE 'America/Sao_Paulo')::time) < '14:00' THEN '1º' ELSE '2º' END
            END
        END,
        $8::timestamptz,
        (($8::timestamptz AT TIME ZONE 'America/Sao_Paulo')::date)
        )
      ON CONFLICT (operador_id, maquina_id, data_ref)
      DO UPDATE SET
        respostas     = EXCLUDED.respostas,
        turno         = EXCLUDED.turno,
        operador_nome = EXCLUDED.operador_nome,
        maquina_nome  = EXCLUDED.maquina_nome,
        updated_at    = now(),
        created_at    = LEAST(public.checklist_submissoes.created_at, EXCLUDED.created_at);
  `;

    const params = [
      user.id,
      operadorNome,
      user.email || '',
      maq.id,
      maquinaNome,
      JSON.stringify(respostas || {}),
      turno,
      createdAtISO,
    ];

    try {
      if (dry) {
        console.log('[DRY-RUN] upsert', { operadorNome, maquinaNome, createdAt: createdAtISO });
      } else {
        await client.query(sql, params);
      }
      upserts++;
    } catch (e) {
      errors++;
      console.error('❌ ERRO upsert doc', doc.id, e.message || e);
    }
  }

  console.log('-------------------------------------------');
  console.log('✅ FIM');
  console.log({ upserts, skipUser, skipMachine, skipTs, errors });

  await client.end();
  process.exit(0);
})().catch(async (err) => {
  console.error('FATAL', err);
  process.exit(1);
});
