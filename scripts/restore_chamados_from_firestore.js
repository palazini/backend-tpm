// scripts/restore_chamados_from_firestore.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const admin = require('firebase-admin');
const crypto = require('crypto');

/* ---------------- util ---------------- */
const norm = (s) =>
  String(s || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .trim();

function normStatus(s) {
  const x = norm(s);
  if (x.startsWith('abert')) return 'Aberto';
  if (x.includes('andament')) return 'Em Andamento';
  if (x.startsWith('conclu')) return 'Concluído';
  if (x.startsWith('cancel')) return 'Cancelado';
  if (x.startsWith('fechad') || x.startsWith('encerr')) return 'Concluído';
  return 'Aberto';
}

function hashStr(str) {
  return crypto.createHash('sha1').update(String(str || '')).digest('hex').slice(0, 16);
}

function buildPg() {
  const connectionString =
    process.env.DATABASE_URL ||
    process.env.PG_URL ||
    (() => {
      const h = process.env.PGHOST || 'localhost';
      const p = process.env.PGPORT || '5432';
      const u = process.env.PGUSER || 'postgres';
      const pw = encodeURIComponent(process.env.PGPASSWORD || '');
      const db = process.env.PGDATABASE || 'postgres';
      return `postgres://${u}:${pw}@${h}:${p}/${db}`;
    })();
  const ssl =
    process.env.PGSSL === 'require' || /supabase|neon|render|heroku/i.test(connectionString)
      ? { rejectUnauthorized: false }
      : undefined;
  return new Client({ connectionString, ssl });
}

function initFirebase() {
  if (!admin.apps.length) {
    try {
      const svc = require('../service-account.json');
      admin.initializeApp({ credential: admin.credential.cert(svc) });
    } catch {
      if (process.env.FB_PROJECT_ID && process.env.FB_CLIENT_EMAIL && process.env.FB_PRIVATE_KEY) {
        admin.initializeApp({
          credential: admin.credential.cert({
            projectId: process.env.FB_PROJECT_ID,
            clientEmail: process.env.FB_CLIENT_EMAIL,
            privateKey: process.env.FB_PRIVATE_KEY.replace(/\\n/g, '\n'),
          }),
        });
      } else {
        console.error('Credenciais do Firebase não encontradas (service-account.json ou envs FB_*).');
        process.exit(1);
      }
    }
  }
  return admin.firestore();
}

function parseArgs() {
  const out = { from: null, to: null, lastDays: 2, dryRun: false, verbose: false, by: 'dataAbertura' };
  for (const a of process.argv.slice(2)) {
    if (a.startsWith('--from=')) out.from = a.slice(7);
    else if (a.startsWith('--to=')) out.to = a.slice(5);
    else if (a.startsWith('--last=')) out.lastDays = parseInt(a.slice(7), 10) || 2;
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--verbose') out.verbose = true;
    else if (a.startsWith('--by=')) out.by = a.slice(5); // 'dataAbertura' (default) ou 'updatedAt'
  }
  return out;
}

function isTestThing({ maqNome, manutentorNome, operadorNome, operadorEmail }) {
  return (
    norm(maqNome).startsWith('teste') ||
    norm(manutentorNome).startsWith('teste') ||
    norm(operadorNome).startsWith('teste') ||
    String(operadorEmail || '').toLowerCase().includes('teste.')
  );
}

/* -------------- MAIN -------------- */
(async () => {
  const fsdb = initFirebase();
  const pg = buildPg();
  await pg.connect();
  await pg.query('select 1');
  const info = await pg.query("select current_database() db, current_user usr, current_schema() sch");
  console.log('✅ Conectado ao Postgres.');
  console.log('DB>', info.rows[0]);

  // Descobrir o esquema real de chamado_observacoes
  const obsColsQ = await pg.query(`
    SELECT column_name, is_nullable
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='chamado_observacoes'
  `);
  const obsCols = Object.fromEntries(obsColsQ.rows.map(r => [r.column_name, r.is_nullable]));
  const hasTextoCol = Object.prototype.hasOwnProperty.call(obsCols, 'texto');
  const hasMensagemCol = Object.prototype.hasOwnProperty.call(obsCols, 'mensagem');
  const autorIdNullable = obsCols['autor_id'] ? obsCols['autor_id'] === 'YES' : true;

  if (!hasTextoCol && !hasMensagemCol) {
    console.error('Tabela public.chamado_observacoes não tem coluna texto ou mensagem — ajuste necessário.');
    process.exit(1);
  }

  // mapas auxiliares
  const usersByName = new Map();
  const usersByEmail = new Map();
  const machinesByName = new Map();

  {
    const { rows } = await pg.query(`SELECT id, nome, lower(email) AS email FROM public.usuarios`);
    rows.forEach((r) => {
      usersByName.set(norm(r.nome), { id: r.id, email: r.email, nome: r.nome });
      if (r.email) usersByEmail.set(String(r.email).toLowerCase(), { id: r.id, email: r.email, nome: r.nome });
    });
  }
  {
    const { rows } = await pg.query(`SELECT id, nome FROM public.maquinas`);
    rows.forEach((r) => machinesByName.set(norm(r.nome), { id: r.id, nome: r.nome }));
  }

  // janela
  const args = parseArgs();
  let { from, to } = args;
  if (!from || !to) {
    const days = Math.max(1, args.lastDays || 2);
    const now = new Date();
    const dFrom = new Date(now);
    dFrom.setDate(dFrom.getDate() - (days - 1));
    from = dFrom.toISOString().slice(0, 10);
    to = now.toISOString().slice(0, 10);
  }
  // usar America/Sao_Paulo (-03:00)
  const start = new Date(`${from}T00:00:00.000-03:00`);
  const end   = new Date(`${to}T23:59:59.999-03:00`);
  console.log(`📥 Buscando chamados de ${from} a ${to} ...`);

  // consulta no Firestore
  const col = fsdb.collection('chamados');
  const field = args.by === 'updatedAt' ? 'updatedAt' : 'dataAbertura';
  const snap = await col.where(field, '>=', start).where(field, '<=', end).get();
  console.log(`🔥 Chamados encontrados: ${snap.size}`);

  let upserts = 0, skips = 0, obsUpserts = 0, errors = 0;

  // fragments dinâmicos p/ observações
  const msgSetFragment =
    (hasTextoCol && hasMensagemCol)
      ? `texto = $5::text, mensagem = $5::text`
      : (hasTextoCol ? `texto = $5::text` : `mensagem = $5::text`);

  const msgColsInsert =
    (hasTextoCol && hasMensagemCol)
      ? `texto, mensagem`
      : (hasTextoCol ? `texto` : `mensagem`);

  const msgValsInsert =
    (hasTextoCol && hasMensagemCol)
      ? `$6::text, $6::text`
      : `$6::text`;

  const updObsSQL = `
    UPDATE public.chamado_observacoes SET
      autor_id    = $2,
      autor_nome  = $3::text,
      autor_email = $4::text,
      ${msgSetFragment},
      criado_em   = $6::timestamptz
    WHERE fs_id    = $1::text
    RETURNING id;
  `;

  const insObsSQL = `
    INSERT INTO public.chamado_observacoes
      (chamado_id, fs_id, autor_id, autor_nome, autor_email, ${msgColsInsert}, criado_em)
    VALUES
      ($1, $2::text, $3, $4::text, $5::text, ${msgValsInsert}, $7::timestamptz)
    RETURNING id;
  `;

  for (const doc of snap.docs) {
    try {
      const d = doc.data() || {};

      const maqNome = String(d.maquina || '').trim();
      const manutentorNome = String(d.manutentorNome || '').trim();
      const operadorNome = String(d.operadorNome || '').trim();
      const operadorEmail = String(d.operadorEmail || '').toLowerCase();

      if (isTestThing({ maqNome, manutentorNome, operadorNome, operadorEmail })) {
        if (args.verbose) console.log('↷ skip teste', { fs_id: doc.id, maqNome, manutentorNome, operadorNome, operadorEmail });
        skips++;
        continue;
      }

      const mach = machinesByName.get(norm(maqNome));
      if (!mach) {
        if (args.verbose) console.log('↷ skip sem máquina conhecida', { fs_id: doc.id, maqNome });
        skips++;
        continue;
      }

      // timestamps
      const tAbert = d.dataAbertura && typeof d.dataAbertura.toDate === 'function' ? d.dataAbertura.toDate() : null;
      const tConc  = d.dataConclusao && typeof d.dataConclusao.toDate === 'function' ? d.dataConclusao.toDate() : null;
      const tUpd   = d.updatedAt && typeof d.updatedAt.toDate === 'function' ? d.updatedAt.toDate() : tConc || tAbert || new Date();

      // campos principais
      const tipo   = String(d.tipo || '').toLowerCase();
      const status = normStatus(d.status);
      const problema = String(d.descricao || '').trim();
      const causa    = String(d.causa || '').trim() || null;
      const servico  = String(d.solucao || '').trim() || null;
      const item     = String(d.item || '').trim() || null;
      const chkKey   = String(d.checklistItemKey || '').trim() || null;

      // atendido_por = manutentor
      const atendUser =
        usersByName.get(norm(manutentorNome)) ||
        usersByEmail.get(String(d.manutentorEmail || '').toLowerCase() || '');
      const atendido_por_id    = atendUser?.id || null;
      const atendido_por_nome  = manutentorNome || atendUser?.nome || null;
      const atendido_por_email = (d.manutentorEmail || atendUser?.email || '').toLowerCase() || null;

      // criado_por = operador
      const criUser =
        usersByEmail.get(operadorEmail) ||
        usersByName.get(norm(operadorNome));
      const criado_por_id    = criUser?.id || null;
      const criado_por_nome  = operadorNome || criUser?.nome || null;
      const criado_por_email = operadorEmail || criUser?.email || null;

      // "Atribuído para" via observação "Sistema"
      let atrib_nome = null, atrib_email = null, atrib_id = null;
      if (Array.isArray(d.observacoes)) {
        for (const o of d.observacoes) {
          if (String(o.autor || '').toLowerCase() === 'sistema') {
            const txt = String(o.texto || '');
            const m = txt.match(/Atribu[ií]do\s+para\s+(.?[^,\.]+?)(?:\s+por\s+.+)?$/i);
            if (m && m[1]) {
              atrib_nome = m[1].trim();
              const au = usersByName.get(norm(atrib_nome));
              if (au) { atrib_id = au.id; atrib_email = au.email; }
            }
          }
        }
      }

      // params comuns (update/insert de chamados)
      const params = [
        doc.id,                // $1  fs_id
        mach.id,               // $2  maquina_id
        tipo || null,          // $3  tipo
        status,                // $4  status
        problema,              // $5  descricao
        problema,              // $6  problema_reportado
        causa,                 // $7  causa
        servico,               // $8  servico_realizado
        criado_por_id,         // $9
        criado_por_nome,       // $10
        criado_por_email,      // $11
        atendido_por_id,       // $12
        atendido_por_nome,     // $13
        atendido_por_email,    // $14
        atrib_id,              // $15
        atrib_nome,            // $16
        atrib_email,           // $17
        tAbert ? tAbert.toISOString() : null, // $18  criado_em
        tUpd   ? tUpd.toISOString()   : null, // $19  atualizado_em
        tConc  ? tConc.toISOString()  : null, // $20  concluido_em
        item,                  // $21
        chkKey,                // $22
      ];

      if (args.dryRun || args.verbose) {
        console.log('[DRY-RUN] chamado full', {
          fs_id: doc.id,
          maquina: maqNome,
          status, tipo,
          criado_em: tAbert?.toISOString(),
          concluido_em: tConc?.toISOString(),
          problema_reportado: problema,
          causa, servico_realizado: servico,
          atendido_por: { nome: atendido_por_nome, email: atendido_por_email },
          criado_por: { nome: criado_por_nome, email: criado_por_email },
          atribuido_para: { nome: atrib_nome, email: atrib_email },
          item, checklist_item_key: chkKey,
        });
      }

      let chamadoId = null;

      if (args.dryRun) {
        chamadoId = '00000000-0000-0000-0000-000000000000';
      } else {
        // UPDATE-then-INSERT (casts explícitos)
        const updSQL = `
          UPDATE public.chamados SET
            maquina_id            = $2,
            tipo                  = $3::text,
            status                = $4::text,
            descricao             = $5::text,
            problema_reportado    = $6::text,
            causa                 = $7::text,
            servico_realizado     = $8::text,
            criado_por_id         = $9,
            criado_por_nome       = $10::text,
            criado_por_email      = $11::text,
            atendido_por_id       = $12,
            atendido_por_nome     = $13::text,
            atendido_por_email    = $14::text,
            atribuido_para_id     = $15,
            atribuido_para_nome   = $16::text,
            atribuido_para_email  = $17::text,
            responsavel_atual_id  = COALESCE($15, responsavel_atual_id),
            criado_em             = COALESCE(LEAST(chamados.criado_em, $18::timestamptz), $18::timestamptz),
            atualizado_em         = GREATEST(COALESCE(chamados.atualizado_em, $19::timestamptz), $19::timestamptz),
            concluido_em          = COALESCE($20::timestamptz, chamados.concluido_em),
            item                  = $21::text,
            checklist_item_key    = $22::text
          WHERE fs_id = $1::text
          RETURNING id;
        `;
        let r = await pg.query(updSQL, params);
        if (r.rowCount) {
          chamadoId = r.rows[0].id;
        } else {
          const insSQL = `
            INSERT INTO public.chamados
              (fs_id, maquina_id, tipo, status, descricao, problema_reportado,
               causa, servico_realizado,
               criado_por_id, criado_por_nome, criado_por_email,
               atendido_por_id, atendido_por_nome, atendido_por_email,
               atribuido_para_id, atribuido_para_nome, atribuido_para_email,
               responsavel_atual_id,
               criado_em, atualizado_em, concluido_em,
               item, checklist_item_key)
            VALUES
              ($1::text, $2, $3::text, $4::text, $5::text, $6::text,
               $7::text, $8::text,
               $9, $10::text, $11::text,
               $12, $13::text, $14::text,
               $15, $16::text, $17::text,
               $15,  -- responsável inicial = atribuído (quando houver)
               $18::timestamptz, $19::timestamptz, $20::timestamptz,
               $21::text, $22::text)
            RETURNING id;
          `;
          r = await pg.query(insSQL, params);
          chamadoId = r.rows[0].id;
        }
      }

      // Observações
      if (Array.isArray(d.observacoes) && d.observacoes.length) {
        for (let i = 0; i < d.observacoes.length; i++) {
          const o = d.observacoes[i] || {};
          const autor_nome = String(o.autor || '').trim() || null;
          const autor_email = String(o.autorEmail || '').toLowerCase() || null;
          const autor =
            usersByEmail.get(autor_email || '') ||
            usersByName.get(norm(autor_nome || ''));
          const criado_em = o.data && typeof o.data.toDate === 'function' ? o.data.toDate() : null;
          const texto = String(o.texto || '').trim();

          if (!criado_em || !texto) {
            if (args.verbose) console.log('  ↷ skip obs sem data ou sem texto', { fs_id: doc.id, i });
            continue;
          }

          // fs_id sintético p/ idempotência
          const obs_fs_id = `${doc.id}:${criado_em.getTime()}:${hashStr(texto)}`;

          // autor_id obrigatório? fallback se preciso
          let autor_id = autor?.id || null;
          if (!autor_id && !autorIdNullable) {
            autor_id = atendido_por_id || atrib_id || criado_por_id || null;
          }
          if (!autor_id && !autorIdNullable) {
            if (args.verbose) console.log('  ↷ skip obs sem autor_id (NOT NULL no schema)', { obs_fs_id });
            continue;
          }

          if (args.dryRun || args.verbose) {
            console.log('  [DRY-RUN] obs', {
              at: criado_em.toISOString(),
              autor: autor_nome,
              msg: texto.slice(0, 200)
            });
          } else {
            // UPDATE-then-INSERT da observação (parametrizado!)
            const upd = await pg.query(updObsSQL, [
              obs_fs_id,               // $1
              autor_id,                // $2
              autor_nome,              // $3
              autor_email,             // $4
              texto,                   // $5 (vai em texto/mensagem)
              criado_em.toISOString(), // $6
            ]);
            if (!upd.rowCount) {
              await pg.query(insObsSQL, [
                chamadoId,               // $1
                obs_fs_id,               // $2
                autor_id,                // $3
                autor_nome,              // $4
                autor_email,             // $5
                texto,                   // $6
                criado_em.toISOString(), // $7
              ]);
            }
            obsUpserts++;
          }
        }
      }

      upserts++;
    } catch (e) {
      errors++;
      console.error('❌ ERRO chamado', doc.id, e?.message || e);
    }
  }

  console.log('------------------------------------');
  console.log('✅ FIM');
  console.log({ from, to, scanned: snap.size, upserts, obsUpserts, skips, errors });

  await pg.end();
  process.exit(0);
})().catch((e) => {
  console.error('FATAL', e);
  process.exit(1);
});
