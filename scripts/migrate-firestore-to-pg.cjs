// scripts/migrate-firestore-to-pg.cjs
/* eslint-disable no-console */
require('dotenv').config();
const { Client } = require('pg');
const { Firestore } = require('@google-cloud/firestore');
const fs = require('fs');
const path = require('path');

/** --------------------------
 *  Config / Helpers
 *  -------------------------- */
const DRY_RUN = ['1', 'true', 'yes'].includes(String(process.env.DRY_RUN || '').toLowerCase());

function logMode() {
  console.log(`>>> MODO MIGRAÇÃO: ${DRY_RUN ? 'DRY-RUN (não grava no banco)' : 'REAL (gravando no banco)'}`);
}

function toDateSafe(v) {
  if (!v) return null;
  try {
    if (typeof v.toDate === 'function') return v.toDate();
    const d = new Date(v);
    return isNaN(d) ? null : d;
  } catch { return null; }
}

function asDocData(doc) {
  // DocumentSnapshot/QueryDocumentSnapshot
  if (doc && typeof doc.data === 'function') {
    return { id: doc.id, data: doc.data() || {} };
  }
  // { id, data } já normalizado
  if (doc && doc.id && doc.data && typeof doc.data !== 'function') {
    return { id: doc.id, data: doc.data || {} };
  }
  // { id, ...campos }
  if (doc && doc.id) {
    const { id, ...rest } = doc;
    return { id, data: rest || {} };
  }
  // dados crus
  return { id: undefined, data: doc || {} };
}

let _fallbackUserId;
async function getFallbackUserId(pg) {
  if (_fallbackUserId) return _fallbackUserId;
  const email = 'import-bot@local';
  const sel = await pg.query(
    'SELECT id FROM usuarios WHERE LOWER(email)=LOWER($1) LIMIT 1',
    [email]
  );
  if (sel.rows.length) {
    _fallbackUserId = sel.rows[0].id;
    return _fallbackUserId;
  }
  const ins = await pg.query(
    'INSERT INTO usuarios (nome,email,role,funcao,usuario) VALUES ($1,$2,$3,$4,$5) RETURNING id',
    ['Import Bot','import-bot@local','gestor','Gestor','import.bot']
  );
  _fallbackUserId = ins.rows[0].id;
  return _fallbackUserId;
}

// cache opcional de mapeamento FS->PG para máquinas (preenchido na etapa de maquinas)
const FS_MAQ_TO_PG = new Map(); // fsMaquinaId -> uuid da maquinas
const FS_USER_TO_PG = new Map();

const norm = (s) => String(s ?? '').trim();
const isObj = (v) => v && typeof v === 'object' && !Array.isArray(v);

function ts(v) {
  try {
    if (!v) return null;
    if (typeof v?.toDate === 'function') return v.toDate().toISOString();
    if (typeof v === 'number') return new Date(v).toISOString();
    const d = new Date(v);
    return isNaN(d) ? null : d.toISOString();
  } catch { return null; }
}

async function getFallbackUserId(pg) {
  // Garante um "Import Bot" para quando não houver autor conhecido
  const email = 'import-bot@local';
  const sel = await pg.query(
    'SELECT id FROM usuarios WHERE LOWER(email)=LOWER($1) LIMIT 1',
    [email]
  );
  if (sel.rows.length) return sel.rows[0].id;
  const ins = await pg.query(
    `INSERT INTO usuarios (nome,email,role,funcao,usuario)
     VALUES ($1,$2,$3,$4,$5) RETURNING id`,
    ['Import Bot', email, 'gestor', 'Gestor', 'import.bot']
  );
  return ins.rows[0].id;
}

async function getUserIdByEmail(pg, email) {
  if (!email) return null;
  const r = await pg.query(
    'SELECT id FROM usuarios WHERE LOWER(email)=LOWER($1) LIMIT 1',
    [email]
  );
  return r.rows.length ? r.rows[0].id : null;
}

async function getUserIdByNomeOrCreate(pg, nome, roleGuess = 'manutentor') {
  const n = norm(nome);
  if (!n) return null;
  // tenta por nome
  const r = await pg.query(
    'SELECT id FROM usuarios WHERE LOWER(nome)=LOWER($1) LIMIT 1',
    [n]
  );
  if (r.rows.length) return r.rows[0].id;
  // cria stub (sem e-mail real)
  const email = `${n.toLowerCase().replace(/\s+/g,'.')}@local`;
  const ins = await pg.query(
    `INSERT INTO usuarios (nome,email,role,funcao,usuario)
     VALUES ($1,$2,$3,$4,$5) RETURNING id`,
    [n, email, roleGuess, roleGuess === 'manutentor' ? 'Técnico Eletromecânico' : 'Operador de CNC', n.toLowerCase().replace(/\s+/g,'.')]
  );
  return ins.rows[0].id;
}

async function findOrCreateMaquinaByNome(pg, nome, fsMaquinaId = null) {
  const n = norm(nome);
  if (!n) return null;

  // tenta por nome exato
  let r = await pg.query(
    'SELECT id FROM maquinas WHERE LOWER(nome)=LOWER($1) LIMIT 1',
    [n]
  );
  if (r.rows.length) return r.rows[0].id;

  // cria stub
  const ins = await pg.query(
    `INSERT INTO maquinas (nome, tag, checklist_diario)
     VALUES ($1, NULL, '{}'::text[])
     RETURNING id`,
    [n]
  );
  const id = ins.rows[0].id;
  if (fsMaquinaId) FS_MAQ_TO_PG.set(fsMaquinaId, id);
  return id;
}

function ts(val) {
  // Converte Firestore Timestamp ou string para Date (ou null)
  if (!val) return null;
  try {
    if (typeof val.toDate === 'function') return val.toDate();
    const d = new Date(val);
    return isNaN(+d) ? null : d;
  } catch {
    return null;
  }
}

async function resolveUsuarioId(pg, { email, fsUserId, nome }) {
  const em = norm(email);
  if (em) {
    const r = await pg.query(`SELECT id FROM usuarios WHERE LOWER(email)=LOWER($1) LIMIT 1`, [em]);
    if (r.rowCount) {
      if (fsUserId) FS_USER_TO_PG.set(fsUserId, r.rows[0].id);
      return r.rows[0].id;
    }
  }
  if (fsUserId && FS_USER_TO_PG.has(fsUserId)) return FS_USER_TO_PG.get(fsUserId);

  // tenta por "usuario" (prefixo do email)
  const usuario = em ? em.split('@')[0] : null;
  if (usuario) {
    const r2 = await pg.query(`SELECT id FROM usuarios WHERE LOWER(usuario)=LOWER($1) LIMIT 1`, [usuario]);
    if (r2.rowCount) {
      if (fsUserId) FS_USER_TO_PG.set(fsUserId, r2.rows[0].id);
      return r2.rows[0].id;
    }
  }

  // cria stub
  const stubEmail   = em || (fsUserId ? `${fsUserId}@migracao.local` : `sem.email+${Date.now()}@migracao.local`);
  const stubUsuario = usuario || (fsUserId || `user_${Math.random().toString(36).slice(2,8)}`);
  const ins = await pg.query(
    `INSERT INTO usuarios (nome, email, usuario, role, funcao, is_deleted)
     VALUES ($1,$2,$3,'operador','Operador de CNC', false)
     ON CONFLICT (email) DO UPDATE SET nome = COALESCE(EXCLUDED.nome, usuarios.nome)
     RETURNING id`,
    [nome || 'Operador (migrado)', stubEmail, stubUsuario]
  );
  const id = ins.rows[0].id;
  if (fsUserId) FS_USER_TO_PG.set(fsUserId, id);
  return id;
}

async function resolveMaquinaId(pg, { fsMaquinaId, maquinaNome }) {
  const f = norm(fsMaquinaId);
  if (f && FS_MAQ_TO_PG.has(f)) return FS_MAQ_TO_PG.get(f);

  const nome = norm(maquinaNome);
  if (nome) {
    const r = await pg.query(`SELECT id FROM maquinas WHERE LOWER(nome)=LOWER($1) LIMIT 1`, [nome]);
    if (r.rowCount) return r.rows[0].id;
    // cria stub com nome e checklist vazio (jsonb)
    const ins = await pg.query(
      `INSERT INTO maquinas (nome, checklist_diario) VALUES ($1,'[]'::jsonb) RETURNING id`,
      [nome]
    );
    const id = ins.rows[0].id;
    if (f) FS_MAQ_TO_PG.set(f, id);
    return id;
  }

  // último recurso: nome "Sem Nome"
  const ins = await pg.query(
    `INSERT INTO maquinas (nome, checklist_diario) VALUES ($1,'[]'::jsonb) RETURNING id`,
    ['Sem Nome (migrado)']
  );
  const id = ins.rows[0].id;
  if (f) FS_MAQ_TO_PG.set(f, id);
  return id;
}

/** --------------------------
 *  Conexões
 *  -------------------------- */
async function connectPg() {
  const cs = process.env.DATABASE_URL;
  const client = cs
    ? new Client({ connectionString: cs })
    : new Client({
        host: process.env.PGHOST || 'localhost',
        port: process.env.PGPORT ? Number(process.env.PGPORT) : 5432,
        database: process.env.PGDATABASE || 'manutencao',
        user: process.env.PGUSER || 'postgres',
        password: process.env.PGPASSWORD || ''
      });
  await client.connect();
  try {
    const r = await client.query(`select current_database() db`);
    console.log('Conectado ao Postgres:', { db: r.rows?.[0]?.db, port: client.port });
  } catch (_) {
    console.log('Conectado ao Postgres.');
  }
  return client;
}

function connectFirestore() {
  const keyPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  if (!keyPath || !fs.existsSync(keyPath)) {
    throw new Error(
      `Credencial do Firebase/Firestore não encontrada. Defina GOOGLE_APPLICATION_CREDENTIALS para o caminho do service-account.json.`
    );
  }
  const sa = JSON.parse(fs.readFileSync(keyPath, 'utf8'));
  const projectId = process.env.FIREBASE_PROJECT_ID || sa.project_id;
  if (!projectId) throw new Error('Não foi possível determinar o projectId do Firestore.');
  return new Firestore({ projectId, keyFilename: keyPath });
}

/** --------------------------
 *  Schema bootstrap (idempotente)
 *  -------------------------- */
async function ensureSchema(pg) {
  const ddl = `
  CREATE EXTENSION IF NOT EXISTS pgcrypto;
  CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

  -- USUARIOS
  DO $$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='usuarios' AND column_name='usuario') THEN
      ALTER TABLE usuarios ADD COLUMN usuario text;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='usuarios' AND column_name='role') THEN
      ALTER TABLE usuarios ADD COLUMN role text;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='usuarios' AND column_name='funcao') THEN
      ALTER TABLE usuarios ADD COLUMN funcao text;
    END IF;
  END$$;

  DO $$
  BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='ux_usuarios_email_plain'
    ) THEN
      CREATE UNIQUE INDEX ux_usuarios_email_plain ON usuarios(email);
    END IF;
  END$$;

  -- MAQUINAS
  DO $$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='maquinas' AND column_name='fs_id') THEN
      ALTER TABLE maquinas ADD COLUMN fs_id text;
      CREATE UNIQUE INDEX ux_maquinas_fs_id ON maquinas(fs_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='maquinas' AND column_name='tag') THEN
      ALTER TABLE maquinas ADD COLUMN tag text;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='maquinas' AND column_name='checklist_diario') THEN
      ALTER TABLE maquinas ADD COLUMN checklist_diario jsonb;
    END IF;
  END$$;

  -- CHAMADOS
  DO $$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='chamados' AND column_name='fs_id') THEN
      ALTER TABLE chamados ADD COLUMN fs_id text;
      CREATE UNIQUE INDEX ux_chamados_fs_id ON chamados(fs_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='chamados' AND column_name='item') THEN
      ALTER TABLE chamados ADD COLUMN item text;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='chamados' AND column_name='checklist_item_key') THEN
      ALTER TABLE chamados ADD COLUMN checklist_item_key text;
    END IF;
  END$$;

  -- CHECKLIST SUBMISSOES
  CREATE TABLE IF NOT EXISTS checklist_submissoes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    fs_id text UNIQUE,
    operador_id uuid NULL REFERENCES usuarios(id) ON DELETE SET NULL,
    operador_nome text,
    operador_email text,
    maquina_id uuid NULL REFERENCES maquinas(id) ON DELETE SET NULL,
    maquina_nome text,
    respostas jsonb NOT NULL DEFAULT '{}'::jsonb,
    turno text,
    criado_em timestamptz NOT NULL DEFAULT now()
  );

  DO $$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='checklist_submissoes' AND column_name='fs_id') THEN
      ALTER TABLE checklist_submissoes ADD COLUMN fs_id text;
      CREATE UNIQUE INDEX ux_checklist_subs_fs_id ON checklist_submissoes(fs_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='checklist_submissoes' AND column_name='operador_id') THEN
      ALTER TABLE checklist_submissoes ADD COLUMN operador_id uuid NULL REFERENCES usuarios(id) ON DELETE SET NULL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='checklist_submissoes' AND column_name='operador_nome') THEN
      ALTER TABLE checklist_submissoes ADD COLUMN operador_nome text;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='checklist_submissoes' AND column_name='operador_email') THEN
      ALTER TABLE checklist_submissoes ADD COLUMN operador_email text;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='checklist_submissoes' AND column_name='maquina_id') THEN
      ALTER TABLE checklist_submissoes ADD COLUMN maquina_id uuid NULL REFERENCES maquinas(id) ON DELETE SET NULL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='checklist_submissoes' AND column_name='maquina_nome') THEN
      ALTER TABLE checklist_submissoes ADD COLUMN maquina_nome text;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='checklist_submissoes' AND column_name='respostas') THEN
      ALTER TABLE checklist_submissoes ADD COLUMN respostas jsonb NOT NULL DEFAULT '{}'::jsonb;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='checklist_submissoes' AND column_name='turno') THEN
      ALTER TABLE checklist_submissoes ADD COLUMN turno text;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='checklist_submissoes' AND column_name='criado_em') THEN
      ALTER TABLE checklist_submissoes ADD COLUMN criado_em timestamptz NOT NULL DEFAULT now();
    END IF;
  END$$;
  `;
  await pg.query(ddl);
}

/** --------------------------
 *  Caches / Lookups
 *  -------------------------- */
const userEmailToUuidCache = new Map();
async function getUserIdByEmail(pg, email) {
  if (!email) return null;
  const k = email.toLowerCase();
  if (userEmailToUuidCache.has(k)) return userEmailToUuidCache.get(k);
  const r = await pg.query(`SELECT id FROM usuarios WHERE LOWER(email)=LOWER($1) LIMIT 1`, [email]);
  const id = r.rows?.[0]?.id || null;
  userEmailToUuidCache.set(k, id);
  return id;
}

const machineFsToUuidCache = new Map();
const machineNameToUuidCache = new Map();
async function mapFsMachineIdToUuid(pg, fsId, nome) {
  if (fsId) {
    if (machineFsToUuidCache.has(fsId)) return machineFsToUuidCache.get(fsId);
    const r = await pg.query(`SELECT id FROM maquinas WHERE fs_id=$1 LIMIT 1`, [fsId]);
    const id = r.rows?.[0]?.id || null;
    machineFsToUuidCache.set(fsId, id);
    if (id) return id;
  }
  const keyName = String(nome || '').trim();
  if (!keyName) return null;
  if (machineNameToUuidCache.has(keyName)) return machineNameToUuidCache.get(keyName);
  const r2 = await pg.query(`SELECT id FROM maquinas WHERE LOWER(nome)=LOWER($1) LIMIT 1`, [keyName]);
  const id2 = r2.rows?.[0]?.id || null;
  machineNameToUuidCache.set(keyName, id2);
  return id2;
}

/** --------------------------
 *  UPSERTS
 *  -------------------------- */
async function upsertUsuario(pg, doc) {
  const fsId = doc.id;
  const d = doc.data() || {};

  const nome    = norm(d.nome) || null;
  const email   = norm(d.email) || null;
  const usuario = norm(d.usuario || (email ? email.split('@')[0] : '')) || null;
  const role    = norm(d.role) || null;
  const funcao  = norm(d.funcao) || (role === 'gestor' ? 'Gestor' : role === 'manutentor' ? 'Técnico Eletromecânico' : 'Operador de CNC');

  const q = await pg.query(
    `INSERT INTO usuarios (nome, email, usuario, role, funcao, is_deleted)
     VALUES ($1,$2,$3,$4,$5,false)
     ON CONFLICT (email) DO UPDATE
       SET nome   = COALESCE(EXCLUDED.nome, usuarios.nome),
           usuario= COALESCE(EXCLUDED.usuario, usuarios.usuario),
           role   = COALESCE(EXCLUDED.role, usuarios.role),
           funcao = COALESCE(EXCLUDED.funcao, usuarios.funcao)
     RETURNING id`,
    [nome, email, usuario, role, funcao]
  );

  const id = q.rows[0].id;
  FS_USER_TO_PG.set(fsId, id);   // <<< importante
  return id;
}

async function upsertMaquina(pg, doc) {
  const fsId = doc.id;
  const d = doc.data() || {};

  const nome = norm(d.nome) || norm(d.nomeMaquina) || norm(d.tag) || fsId;
  const tag  = d.tag ? norm(d.tag) : null;

  // Firestore → array JS → JSONB
  const checklistArr = Array.isArray(d.checklistDiario)
    ? d.checklistDiario.map(String)
    : [];

  // procura por tag (se existir) ou nome
  let row;
  if (tag) {
    row = await pg.query('SELECT id FROM maquinas WHERE tag = $1 LIMIT 1', [tag]);
  } else {
    row = await pg.query('SELECT id FROM maquinas WHERE LOWER(nome)=LOWER($1) LIMIT 1', [nome]);
  }

  let id;
  if (row.rows.length) {
    id = row.rows[0].id;
    await pg.query(
      `UPDATE maquinas
          SET nome = COALESCE($1, nome),
              tag  = COALESCE($2, tag),
              checklist_diario = $3::jsonb
        WHERE id = $4`,
      [nome || null, tag, JSON.stringify(checklistArr), id]
    );
  } else {
    const ins = await pg.query(
      `INSERT INTO maquinas (nome, tag, checklist_diario)
       VALUES ($1,$2,$3::jsonb)
       RETURNING id`,
      [nome, tag, JSON.stringify(checklistArr)]
    );
    id = ins.rows[0].id;
  }

  // salvar o mapeamento FS -> PG para usar depois nos chamados/submissões
  FS_MAQ_TO_PG.set(fsId, id);
  return id;
}

async function upsertChamado(pg, doc) {
  const fsId = doc.id;
  const d = doc.data() || {};

  // Campos principais vindos do Firestore
  const maquinaNome = norm(d.maquina);
  const fsMaquinaId = norm(d.maquinaId);
  const tipoRaw = String(d.tipo || '').toLowerCase();
  const tipo = ['corretiva','preditiva','preventiva'].includes(tipoRaw) ? tipoRaw : 'corretiva';
  const status = norm(d.status) || 'Aberto';
  const descricao = norm(d.descricao) || null;

  const item = norm(d.item) || null;
  const checklistKey = norm(d.checklistItemKey) || null;
  const causa = norm(d.causa) || null;
  const solucao = norm(d.solucao) || null;

  const operadorEmail = norm(d.operadorEmail);
  const operadorNome  = norm(d.operadorNome);

  const manutentorNome = norm(d.manutentorNome); // Firestore não traz e-mail do manutentor
  // origin/observacoes
  const origin = norm(d.origin) || null;
  const observacoesJs = Array.isArray(d.observacoes)
    ? d.observacoes.map((o) => ({
        autor: o?.autor || null,
        texto: o?.texto || null,
        data:  ts(o?.data)?.toISOString() || null
      })).filter(x => x.autor || x.texto || x.data)
    : [];

  // Datas
  const ab = ts(d.dataAbertura);
  const co = ts(d.dataConclusao);
  const up = ts(d.updatedAt);

  // Máquina: usar mapa por FS-id OU nome. Se não achar, cria stub.
  let maquinaId = null;
  if (fsMaquinaId && FS_MAQ_TO_PG.has(fsMaquinaId)) {
    maquinaId = FS_MAQ_TO_PG.get(fsMaquinaId);
  } else if (maquinaNome) {
    maquinaId = await findOrCreateMaquinaByNome(pg, maquinaNome, fsMaquinaId || null);
  }

  // Autor do chamado (criado_por_id): tenta operadorEmail; se não houver, cria fallback
  let criadoPorId = await getUserIdByEmail(pg, operadorEmail);
  if (!criadoPorId && operadorNome) {
    criadoPorId = await getUserIdByNomeOrCreate(pg, operadorNome, 'operador');
  }
  if (!criadoPorId) {
    criadoPorId = await getFallbackUserId(pg);
  }

  // Manutentor (opcional)
  let manutentorId = null;
  if (manutentorNome) {
    manutentorId = await getUserIdByNomeOrCreate(pg, manutentorNome, 'manutentor');
  }

  // UPSERT pelo fs_id
  await pg.query(
    `
    INSERT INTO chamados
      (fs_id, maquina_id, tipo, status, descricao, criado_por_id,
       manutentor_id, item, checklist_item_key, causa, solucao,
       observacoes, criado_em, concluido_em, updated_em)
    VALUES
      ($1,    $2,         $3,   $4,     $5,       $6,
       $7,           $8,   $9,                $10,   $11,
       $12::jsonb,   $13,       $14,         $15)
    ON CONFLICT (fs_id) DO UPDATE SET
      maquina_id         = EXCLUDED.maquina_id,
      tipo               = EXCLUDED.tipo,
      status             = EXCLUDED.status,
      descricao          = EXCLUDED.descricao,
      criado_por_id      = EXCLUDED.criado_por_id,
      manutentor_id      = EXCLUDED.manutentor_id,
      item               = EXCLUDED.item,
      checklist_item_key = EXCLUDED.checklist_item_key,
      causa              = EXCLUDED.causa,
      solucao            = EXCLUDED.solucao,
      observacoes        = EXCLUDED.observacoes,
      criado_em          = EXCLUDED.criado_em,
      concluido_em       = EXCLUDED.concluido_em,
      updated_em         = EXCLUDED.updated_em
    `,
    [
      fsId || null,
      maquinaId || null,
      tipo,
      status,
      descricao,
      criadoPorId,
      manutentorId,
      item,
      checklistKey,
      causa,
      solucao,
      JSON.stringify(observacoesJs),
      ab ? ab.toISOString() : null,
      co ? co.toISOString() : null,
      up ? up.toISOString() : null
    ]
  );
}

async function upsertChecklistSubmission(pg, doc) {
  const fsId = doc.id;
  const d = doc.data ? doc.data() : doc; // compat

  const operadorEmail = norm(d.operadorEmail);
  const operadorNome  = norm(d.operadorNome);
  const fsOperadorId  = norm(d.operadorId);

  const fsMaquinaId   = norm(d.maquinaId);
  const maquinaNome   = norm(d.maquinaNome || d.maquina);

  const operadorId = await resolveUsuarioId(pg, {
    email: operadorEmail, fsUserId: fsOperadorId, nome: operadorNome
  });

  const maquinaId = await resolveMaquinaId(pg, {
    fsMaquinaId, maquinaNome
  });

  const respostas = isObj(d.respostas) ? d.respostas : {};
  const turno     = norm(d.turno);
  const criadoEm  = ts(d.dataSubmissao || d.createdAt);

  await pg.query(
    `INSERT INTO checklist_submissoes
       (fs_id, operador_id, operador_nome, operador_email,
        maquina_id, maquina_nome, respostas, turno, criado_em)
     VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,COALESCE($9::timestamptz, NOW()))
     ON CONFLICT (fs_id) DO UPDATE SET
       operador_id  = EXCLUDED.operador_id,
       operador_nome= EXCLUDED.operador_nome,
       operador_email=EXCLUDED.operador_email,
       maquina_id   = EXCLUDED.maquina_id,
       maquina_nome = EXCLUDED.maquina_nome,
       respostas    = EXCLUDED.respostas,
       turno        = EXCLUDED.turno,
       criado_em    = EXCLUDED.criado_em
    `,
    [
      fsId,
      operadorId,
      operadorNome || null,
      operadorEmail || null,
      maquinaId,
      maquinaNome || null,
      JSON.stringify(respostas),
      turno || null,
      criadoEm
    ]
  );
}

/** --------------------------
 *  MIGRAÇÕES
 *  -------------------------- */
async function migrateUsuarios(db, pg) {
  const snap = await db.collection('usuarios').get();
  console.log(`- usuarios: ${snap.size} docs`);
  for (const doc of snap.docs) {
    await upsertUsuario(pg, doc);
  }
  console.log('  ->', snap.size, 'processados.');
}

async function migrateMaquinas(db, pg) {
  const snap = await db.collection('maquinas').get();
  console.log(`- maquinas: ${snap.size} docs`);
  for (const doc of snap.docs) {
    try {
      await upsertMaquina(pg, doc);
    } catch (e) {
      console.warn(`  ! falhou maquinas/${doc.id}: ${e.message || e}`);
    }
  }
  console.log('  ->', snap.size, 'processados.');
}


async function migrateChamados(db, pg) {
  const snap = await db.collection('chamados').get();
  console.log(`- chamados: ${snap.size} docs`);
  for (const doc of snap.docs) {
    await upsertChamado(pg, doc);
  }
  console.log('  ->', snap.size, 'processados.');
}

async function migrateChecklistSubmissions(db, pg) {
  const snap = await db.collection('checklistSubmissions').get();
  console.log(`- checklistSubmissions: ${snap.size} docs`);
  for (const doc of snap.docs) {
    await upsertChecklistSubmission(pg, doc);
  }
  console.log('  ->', snap.size, 'processados.');
}

/** --------------------------
 *  MAIN
 *  -------------------------- */
(async () => {
  try {
    logMode();
    const pg = await connectPg();
    const db = connectFirestore();

    await ensureSchema(pg);

    await migrateUsuarios(db, pg);
    await migrateMaquinas(db, pg);
    await migrateChamados(db, pg);
    await migrateChecklistSubmissions(db, pg);

    await pg.end();
  } catch (e) {
    console.error('Falha na migração:', e);
    process.exit(1);
  }
})();
