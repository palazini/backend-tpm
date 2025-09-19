import { Router } from 'express';
import { pool } from '../db';
import { sseBroadcast } from '../utils/sse';

export const maquinasRouter = Router();

maquinasRouter.get("/maquinas", async (req, res) => {
  try {
    const q = (req.query.q as string | undefined)?.trim();
    const params: any[] = [];
    let where = "1=1";
    if (q) {
      params.push(`%${q}%`);
      where = "(nome ILIKE $" + params.length + " OR tag ILIKE $" + params.length + ")";
    }

    const { rows } = await pool.query(
      `SELECT id, nome, tag, setor, critico
       FROM maquinas
       WHERE ${where}
       ORDER BY nome ASC`,
      params
    );
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});



// Criar mÃ¡quina
maquinasRouter.post("/maquinas", async (req, res) => {
  try {
    const { nome, tag, setor, critico } = req.body ?? {};
    const nomeTrim = String(nome || "").trim();
    const tagTrim = String(tag || nomeTrim).trim();

    if (!nomeTrim || nomeTrim.length < 2) {
      return res.status(400).json({ error: "Nome da mÃ¡quina Ã© obrigatÃ³rio." });
    }

    // Evita duplicado (mesmo sem UNIQUE no banco)
    const dup = await pool.query(
      `SELECT id FROM maquinas
        WHERE lower(nome) = lower($1) OR lower(tag) = lower($2)
        LIMIT 1`,
      [nomeTrim, tagTrim]
    );
    if ((dup.rowCount ?? 0) > 0) {
      return res.status(409).json({ error: "JÃ¡ existe uma mÃ¡quina com esse nome/tag." });
    }

    const { rows } = await pool.query(
      `INSERT INTO maquinas (nome, tag, setor, critico)
       VALUES ($1, $2, $3, COALESCE($4, false))
       RETURNING id, nome, tag, setor, critico`,
      [nomeTrim, tagTrim, setor ?? null, !!critico]
    );

    // SSE broadcast
    sseBroadcast({ topic: "maquinas", action: "created", id: rows[0].id });

    res.status(201).json(rows[0]);
  } catch (e: any) {
    console.error(e);
    // Se vocÃª tiver UNIQUE no banco, pode cair aqui:
    if (e?.code === "23505") {
      return res.status(409).json({ error: "JÃ¡ existe uma mÃ¡quina com esse nome/tag." });
    }
    res.status(500).json({ error: String(e) });
  }
});



maquinasRouter.get('/maquinas/:id', async (req, res) => {
  try {
    const id = String(req.params.id);
    const TZ = 'America/Sao_Paulo';

    // 1) Dados da mÃ¡quina
    const maq = await pool.query(
      `
      SELECT
        id,
        nome,
        tag,
        setor,
        critico,
        COALESCE(checklist_diario, '[]'::jsonb) AS checklist_diario
      FROM maquinas
      WHERE id = $1
      `,
      [id]
    );
    if (!maq.rowCount) {
      return res.status(404).json({ error: 'MÃ¡quina nÃ£o encontrada.' });
    }

    // 2) Chamados ATIVOS (Aberto/Em Andamento)
    const ativos = await pool.query(
      `
      SELECT
        c.id,
        c.tipo,
        c.status,
        c.descricao,
        c.item,
        c.checklist_item_key,
        to_char(c.criado_em, 'YYYY-MM-DD HH24:MI') AS criado_em
      FROM chamados c
      WHERE c.maquina_id = $1
        AND c.status IN ('Aberto','Em Andamento')
      ORDER BY c.criado_em DESC
      LIMIT 50
      `,
      [id]
    );

    // 3) Ãšltimas submissÃµes de checklist (com totais e qtd de "nao")
    //    Usamos LATERAL para calcular total/nao em uma Ãºnica varredura.
        const subms = await pool.query(
      `
      SELECT
        s.id,
        s.maquina_id,
        s.operador_nome,
        s.operador_email,
        COALESCE(NULLIF(s.turno,''),'') AS turno,
        s.respostas,
        to_char(COALESCE(s.created_at, s.criado_em), 'YYYY-MM-DD HH24:MI') AS criado_em,
        stats.total_itens,
        stats.itens_nao
      FROM checklist_submissoes s
      LEFT JOIN LATERAL (
        SELECT
          COUNT(*)                                        AS total_itens,
          COUNT(*) FILTER (WHERE e.value = 'nao')         AS itens_nao
        FROM jsonb_each_text(s.respostas) AS e
      ) AS stats ON TRUE
      WHERE s.maquina_id = $1
      ORDER BY COALESCE(s.created_at, s.criado_em) DESC
      LIMIT 50
      `,
      [id]
    );

    // 4) histÃ³rico agregado por dia/turno (para a tabela "histÃ³rico de Conformidade DiÃ¡ria")
        const historico = await pool.query(
      `
      WITH base AS (
        SELECT
          (COALESCE(created_at, criado_em) AT TIME ZONE $2)  AS dt,
          COALESCE(NULLIF(turno,''),'')                      AS turno_raw,
          COALESCE(operador_nome,'')                         AS operador_nome
        FROM checklist_submissoes
        WHERE maquina_id = $1
      ),
      norm AS (
        SELECT
          dt::date AS dia,
          /* normaliza: aceita 1, 1Âº, 1o, 1Â°, primeiro, turno1; idem para 2 */
          CASE
            WHEN lower(turno_raw) IN ('1','1Âº','1o','1Â°','primeiro','turno1') THEN '1Âº'
            WHEN lower(turno_raw) IN ('2','2Âº','2o','2Â°','segundo','turno2')   THEN '2Âº'
            WHEN turno_raw = '' THEN CASE WHEN EXTRACT(HOUR FROM dt) < 14 THEN '1Âº' ELSE '2Âº' END
            ELSE CASE
              WHEN regexp_replace(lower(turno_raw), '[^0-9]', '', 'g') = '1' THEN '1Âº'
              WHEN regexp_replace(lower(turno_raw), '[^0-9]', '', 'g') = '2' THEN '2Âº'
              ELSE CASE WHEN EXTRACT(HOUR FROM dt) < 14 THEN '1Âº' ELSE '2Âº' END
            END
          END AS turno_norm,
          operador_nome
        FROM base
      )
      SELECT
        to_char(dia, 'YYYY-MM-DD') AS dia,
        (COUNT(*) FILTER (WHERE turno_norm = '1Âº') > 0)::bool AS turno1_ok,
        (COUNT(*) FILTER (WHERE turno_norm = '2Âº') > 0)::bool AS turno2_ok,
        COALESCE(string_agg(DISTINCT operador_nome, ', ')
                 FILTER (WHERE turno_norm = '1Âº'), '') AS turno1_operadores,
        COALESCE(string_agg(DISTINCT operador_nome, ', ')
                 FILTER (WHERE turno_norm = '2Âº'), '') AS turno2_operadores
      FROM norm
      GROUP BY 1
      ORDER BY 1 DESC
      LIMIT 60
      `,
      [id, TZ]
    );

    // 5) Resposta
    res.json({
      ...maq.rows[0],
      chamadosAtivos: ativos.rows,       // cards "Chamados Ativos"
      checklistHistorico: subms.rows,    // lista das Ãºltimas submissÃµes (com totais)
      historicoChecklist: historico.rows // agregado por dia/turno p/ a tabela do painel
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// ADICIONAR ITEM AO CHECKLIST DIÃRIO DA MÃQUINA


maquinasRouter.post('/maquinas/:id/checklist-add', async (req, res) => {
  try {
    const id   = String(req.params.id);
    const item = String(req.body?.item || '').trim();

    if (!item) return res.status(400).json({ error: 'Item invÃ¡lido.' });

    // evita duplicados (case-insensitive) e adiciona no final
    const { rows } = await pool.query(
      `
      UPDATE maquinas
         SET checklist_diario =
               CASE
                 WHEN EXISTS (
                   SELECT 1
                     FROM jsonb_array_elements_text(COALESCE(checklist_diario,'[]'::jsonb)) AS x(val)
                    WHERE lower(val) = lower($2)
                 )
                 THEN COALESCE(checklist_diario,'[]'::jsonb)
                 ELSE COALESCE(checklist_diario,'[]'::jsonb) || to_jsonb(ARRAY[$2]::text[])
               END
       WHERE id = $1
       RETURNING COALESCE(checklist_diario,'[]'::jsonb) AS checklist_diario;
      `,
      [id, item]
    );

    if (!rows.length) return res.status(404).json({ error: 'MÃ¡quina nÃ£o encontrada.' });
    res.json({ checklistDiario: rows[0].checklist_diario });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// REMOVER ITEM DO CHECKLIST DIÃRIO DA MÃQUINA
maquinasRouter.post('/maquinas/:id/checklist-remove', async (req, res) => {
  try {
    const id   = String(req.params.id);
    const item = String(req.body?.item || '').trim();
    if (!item) return res.status(400).json({ error: 'Item invÃ¡lido.' });

    const { rows } = await pool.query(
      `
      UPDATE maquinas
         SET checklist_diario = (
               SELECT COALESCE(jsonb_agg(to_jsonb(val)), '[]'::jsonb)
                 FROM jsonb_array_elements_text(COALESCE(checklist_diario,'[]'::jsonb)) AS x(val)
                WHERE lower(val) <> lower($2)
             )
       WHERE id = $1
       RETURNING COALESCE(checklist_diario,'[]'::jsonb) AS checklist_diario;
      `,
      [id, item]
    );

    if (!rows.length) return res.status(404).json({ error: 'MÃ¡quina nÃ£o encontrada.' });
    res.json({ checklistDiario: rows[0].checklist_diario });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// CRIAR PEÃ‡A (somente gestor)

