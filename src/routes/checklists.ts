import { Router } from 'express';
import { pool } from '../db';
import { slugify } from '../utils/slug';
import { sseBroadcast } from '../utils/sse';

export const checklistsRouter = Router();

checklistsRouter.post('/checklists/daily/submit', async (req, res) => {
  try {
    const auth = (req as any).user || {};
    if (!auth?.email) return res.status(401).json({ error: 'Sem Usuário no header.' });

    const {
      operadorEmail = auth.email,
      operadorNome = '',
      maquinaId,
      maquinaNome = '',
      respostas = {},
      turno = ''
    } = req.body || {};

    if (!operadorEmail || !maquinaId || !respostas || typeof respostas !== 'object') {
      return res.status(400).json({ error: 'Dados inválidos.' });
    }

    // operador
    const u = await pool.query(`SELECT id, nome FROM usuarios WHERE email = $1`, [operadorEmail]);
    if (!u.rowCount) return res.status(404).json({ error: 'Operador não encontrado.' });
    const operadorId = u.rows[0].id;
    const operadorNomeFinal = operadorNome || u.rows[0].nome || '';

    // máquina
    const m = await pool.query(`SELECT id, nome FROM maquinas WHERE id = $1`, [maquinaId]);
    if (!m.rowCount) return res.status(404).json({ error: 'Máquina não encontrada.' });
    const maquinaNomeFinal = maquinaNome || m.rows[0].nome || '';

    // 1) grava submissão
    await pool.query(
      `INSERT INTO checklist_submissoes
        (operador_id, operador_nome, operador_email,
          maquina_id,  maquina_nome,  respostas, turno, created_at, data_ref)
      VALUES (
        $1,$2,$3,$4,$5,$6::jsonb,
        /* turno normalizado */
        CASE
          WHEN lower($7) IN ('turno1','1','1º','1o','1°','primeiro') THEN '1º'
          WHEN lower($7) IN ('turno2','2','2º','2o','2°','segundo')   THEN '2º'
          WHEN coalesce($7,'') = '' THEN
            CASE WHEN (now() AT TIME ZONE 'America/Sao_Paulo')::time < '14:00' THEN '1º' ELSE '2º' END
          ELSE
            CASE
              WHEN regexp_replace(lower($7),'[^0-9]','','g') = '1' THEN '1º'
              WHEN regexp_replace(lower($7),'[^0-9]','','g') = '2' THEN '2º'
              ELSE CASE WHEN (now() AT TIME ZONE 'America/Sao_Paulo')::time < '14:00' THEN '1º' ELSE '2º' END
            END
        END,
        now(),
        (now() AT TIME ZONE 'America/Sao_Paulo')::date
      )
      ON CONFLICT (operador_id, maquina_id, data_ref)
      DO UPDATE SET
        respostas     = EXCLUDED.respostas,
        turno         = EXCLUDED.turno,
        operador_nome = EXCLUDED.operador_nome,
        maquina_nome  = EXCLUDED.maquina_nome,
        updated_at    = now()`
      ,
      [operadorId, operadorNomeFinal, operadorEmail, maquinaId, maquinaNomeFinal, JSON.stringify(respostas), turno]
    );

    // 2) cria chamados preditivos para itens 'nao' (sem duplicar)
    let gerados = 0;
    for (const [pergunta, valor] of Object.entries(respostas as Record<string,string>)) {
      if (valor !== 'nao') continue;
      const key = slugify(pergunta);

      // já existe aberto/andamento para este item desta máquina?
      const { rows: ja } = await pool.query(
        `SELECT 1
           FROM chamados
          WHERE maquina_id = $1
            AND tipo = 'preditiva'
            AND checklist_item_key = $2
            AND status IN ('Aberto','Em Andamento')
          LIMIT 1`,
        [maquinaId, key]
      );
      if (ja.length) continue;

      const descricao = `Checklist: item "${pergunta}" marcado como NÃO.`;

      await pool.query(
        `INSERT INTO chamados
           (maquina_id, tipo, status, descricao, criado_por_id, item, checklist_item_key)
         VALUES ($1, 'preditiva', 'Aberto', $2, $3, $4, $5)`,
        [maquinaId, descricao, operadorId, pergunta, key]
      );

      try { sseBroadcast?.({ topic: 'chamados', action: 'created' }); } catch {}
      gerados++;
    }

    res.json({ ok: true, chamados_gerados: gerados });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});


checklistsRouter.get('/checklists/daily/submissoes', async (req, res) => {
  try {
    const operadorEmail = String(req.query.operadorEmail || '')
      .trim()
      .toLowerCase();           // normalize no parâmetro
    const dateISO = String(req.query.date || '').slice(0, 10); // 'YYYY-MM-DD'

    if (!operadorEmail || !dateISO) {
      return res.status(400).json({ error: 'Informe operadorEmail e date (YYYY-MM-DD).' });
    }

    // janela do dia em UTC: [date 00:00Z, date+1 00:00Z)
    const start = `${dateISO}T00:00:00Z`;

    const { rows } = await pool.query(
      `SELECT
        id,
        operador_id,
        operador_nome,
        operador_email,
        maquina_id,
        maquina_nome,
        respostas,
        turno,
        created_at
      FROM checklist_submissoes
      WHERE operador_email = $1
        AND created_at >= ($2::date AT TIME ZONE 'America/Sao_Paulo')
        AND created_at <  (($2::date + interval '1 day') AT TIME ZONE 'America/Sao_Paulo')
      ORDER BY created_at DESC`,
      [operadorEmail, dateISO]  // aqui $2 é só 'YYYY-MM-DD'
    );

    res.json({ items: rows });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

