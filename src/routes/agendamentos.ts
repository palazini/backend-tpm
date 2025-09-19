import { Router } from 'express';
import { pool } from '../db';
import { slugify } from '../utils/slug';
import { sseBroadcast } from '../utils/sse';

export const agendamentosRouter = Router();

/* =========================
   2.2 — AGENDAMENTOS PREVENTIVOS
   ========================= */

// Listar agendamentos (por janela e/ou limite)
agendamentosRouter.get("/agendamentos", async (req, res) => {
  try {
    const { from, to, limit, order } = req.query as any;
    const params: any[] = [];
    const where: string[] = [];

    if (from) {
      params.push(from);
      where.push(`a.start_ts >= $${params.length}`);
    }
    if (to) {
      params.push(to);
      where.push(`a.end_ts   <= $${params.length}`);
    }

    const whereSql = where.length ? where.join(" AND ") : "1=1";
    const lim = Math.min(Math.max(parseInt(limit || "0", 10) || 0, 0), 500);
    const orderSql = order === "recent" ? "a.criado_em DESC" : "a.start_ts ASC";

    const sql = `
      SELECT
        a.id,
        a.maquina_id,
        m.nome AS maquina_nome,
        a.descricao,
        a.itens_checklist,
        a.original_start, a.original_end,
        a.start_ts, a.end_ts,
        a.status,
        a.criado_em,
        a.concluido_em,
        (a.status = 'concluido' AND a.concluido_em > a.end_ts) AS atrasado
      FROM agendamentos_preventivos a
      JOIN maquinas m ON m.id = a.maquina_id
      WHERE ${whereSql}
      ORDER BY ${orderSql}
      ${lim > 0 ? `LIMIT ${lim}` : ""}
    `;
    const { rows } = await pool.query(sql, params);
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// Criar agendamento
agendamentosRouter.post("/agendamentos", async (req, res) => {
  try {
    const { maquinaId, descricao, itensChecklist, start, end } = req.body ?? {};
    if (!maquinaId || !descricao || !start || !end) {
      return res.status(400).json({
        error: "Campos obrigatórios: maquinaId, descricao, start, end."
      });
    }

    // 1) Normaliza itensChecklist para array de OBJETOS {texto, key}
    type Item = { texto: string; key: string };
    let itens: Item[] = [];

    const toObj = (value: any, idx: number): Item | null => {
      // já é objeto com texto
      if (value && typeof value === "object" && value.texto) {
        const texto = String(value.texto).trim();
        if (!texto) return null;
        const key = value.key ? slugify(String(value.key)) : slugify(texto || String(idx));
        return { texto, key };
      }
      // string/number -> objeto
      const texto = String(value ?? "").trim();
      if (!texto) return null;
      return { texto, key: slugify(texto || String(idx)) };
    };

    if (Array.isArray(itensChecklist)) {
      itens = itensChecklist
        .map((v, i) => toObj(v, i))
        .filter(Boolean) as Item[];
    } else if (typeof itensChecklist === "string") {
      // aceita JSON (["Item A","Item B"]) OU linhas de textarea
      let parsed: any = null;
      try { parsed = JSON.parse(itensChecklist); } catch {}
      const arr = Array.isArray(parsed)
        ? parsed
        : String(itensChecklist)
            .split(/\r?\n/)
            .map(s => s.trim())
            .filter(Boolean);
      itens = arr
        .map((v, i) => toObj(v, i))
        .filter(Boolean) as Item[];
    } else {
      itens = []; // sem checklist
    }

    // 2) Insere (jsonb de objetos)
    const { rows } = await pool.query(
      `INSERT INTO agendamentos_preventivos
         (maquina_id, descricao, itens_checklist, original_start, original_end, start_ts, end_ts, status)
       VALUES ($1, $2, $3::jsonb, $4, $5, $4, $5, 'agendado')
       RETURNING id`,
      [maquinaId, String(descricao).trim(), JSON.stringify(itens), start, end]
    );

    const id = rows[0].id;

    // 3) Retorna o registro completo
    const { rows: out } = await pool.query(
      `SELECT a.id, a.maquina_id, m.nome AS maquina_nome, a.descricao, a.itens_checklist,
              a.original_start, a.original_end, a.start_ts, a.end_ts, a.status,
              a.criado_em, a.concluido_em,
              (a.status='concluido' AND a.concluido_em > a.end_ts) AS atrasado
         FROM agendamentos_preventivos a
         JOIN maquinas m ON m.id = a.maquina_id
        WHERE a.id = $1`,
      [id]
    );

    try { sseBroadcast?.({ topic: "agendamentos", action: "created", id: out[0].id }); } catch {}

    res.status(201).json(out[0]);
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// Atualizar (reagendar) - gestor
agendamentosRouter.patch("/agendamentos/:id", async (req, res) => {
  try {
    const role = (req as any).user?.role ?? "operador";
    if (role !== "gestor") return res.status(403).json({ error: "Somente gestor pode reagendar." });

    const id = String(req.params.id);
    const { start, end, status } = req.body ?? {};
    const sets: string[] = [];
    const params: any[] = [];

    if (start) {
      params.push(start);
      sets.push(`start_ts = $${params.length}`);
    }
    if (end) {
      params.push(end);
      sets.push(`end_ts   = $${params.length}`);
    }
    if (status && ["agendado", "iniciado", "concluido"].includes(status)) {
      params.push(status);
      sets.push(`status = $${params.length}`);
      if (status === "concluido") {
        sets.push(`concluido_em = NOW()`);
      }
    }

    if (!sets.length) return res.status(400).json({ error: "Nada para atualizar." });
    params.push(id);

    const { rowCount } = await pool.query(
      `UPDATE agendamentos_preventivos SET ${sets.join(", ")} WHERE id = $${params.length}`,
      params
    );
    if (!rowCount) return res.status(404).json({ error: "Agendamento não encontrado." });

    // SSE broadcast
    sseBroadcast({ topic: "agendamentos", action: "updated", id });

    res.json({ ok: true });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// Deletar - gestor
agendamentosRouter.delete("/agendamentos/:id", async (req, res) => {
  try {
    const role = (req as any).user?.role ?? "operador";
    if (role !== "gestor") return res.status(403).json({ error: "Somente gestor pode deletar." });

    const id = String(req.params.id);
    const { rowCount } = await pool.query(
      `DELETE FROM agendamentos_preventivos WHERE id = $1`,
      [id]
    );
    if (!rowCount) return res.status(404).json({ error: "Agendamento não encontrado." });

    // SSE broadcast
    sseBroadcast({ topic: "agendamentos", action: "deleted", id });

    res.json({ ok: true });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// Iniciar manutenção (gera chamado preventivo + muda status)
agendamentosRouter.post("/agendamentos/:id/iniciar", async (req, res) => {
  const client = await pool.connect();
  try {
    const user = (req as any).user as { role?: string; email?: string } | undefined;
    const role = user?.role ?? "operador";
    if (!(role === "manutentor" || role === "gestor")) {
      return res.status(403).json({ error: "Apenas manutentor/gestor podem iniciar manutenção." });
    }

    const id = String(req.params.id);

    // 1) Buscar agendamento + máquina (trazer itens_checklist como jsonb)
    const { rows } = await client.query(
      `SELECT a.id,
              a.maquina_id,
              a.descricao,
              COALESCE(a.itens_checklist, '[]'::jsonb) AS itens_checklist,
              m.nome AS maquina_nome
         FROM agendamentos_preventivos a
         JOIN maquinas m ON m.id = a.maquina_id
        WHERE a.id = $1
        FOR UPDATE`,
      [id]
    );
    const ag = rows[0];
    if (!ag) return res.status(404).json({ error: "Agendamento não encontrado." });

    // 2) Quem cria e (opcional) quem será o responsável inicial
    const criadoPorEmail  = req.body?.criadoPorEmail || user?.email;
    const manutentorEmail = req.body?.manutentorEmail || null;
    if (!criadoPorEmail) {
      return res.status(400).json({ error: "Informe criadoPorEmail." });
    }

    await client.query("BEGIN");

    const { rows: uCriador } = await client.query(
      `SELECT id FROM usuarios WHERE LOWER(email)=LOWER($1) LIMIT 1`,
      [criadoPorEmail]
    );
    if (!uCriador.length) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "Usuário (criadoPorEmail) não existe em usuarios." });
    }

    let manutentorId: string | null = null;
    if (manutentorEmail) {
      const { rows: uMant } = await client.query(
        `SELECT id FROM usuarios WHERE LOWER(email)=LOWER($1) LIMIT 1`,
        [manutentorEmail]
      );
      manutentorId = uMant[0]?.id ?? null;
    }

    // 3) Normaliza itens_checklist -> [{ texto, key }]
    const itensRaw: any[] = Array.isArray(ag.itens_checklist) ? ag.itens_checklist : [];
    const checklist = itensRaw.map((it, idx) => {
      if (it && typeof it === "object" && it.texto) {
        const texto = String(it.texto).trim();
        const key = it.key ? slugify(String(it.key)) : slugify(texto || String(idx));
        return { texto, key };
      }
      const texto = String(it ?? "").trim();
      return { texto, key: slugify(texto || String(idx)) };
    });

    // 4) Status inicial
    const statusInicial = manutentorId ? "Em Andamento" : "Aberto";

    // 5) Criar chamado preventivo já com checklist (jsonb) e tipo_checklist (text[])
    const descricaoChamado = `Preventiva: ${ag.descricao || ag.maquina_nome}`.trim();
    const { rows: crows } = await client.query(
      `INSERT INTO chamados
         (maquina_id, tipo, status, descricao,
          criado_por_id, manutentor_id, responsavel_atual_id,
          checklist, tipo_checklist)
       VALUES
         ($1, 'preventiva', $2, $3,
          $4, $5, $6,
          $7::jsonb, ARRAY['preventiva']::text[])
       RETURNING id`,
      [
        ag.maquina_id,
        statusInicial,
        descricaoChamado,
        uCriador[0].id,
        manutentorId,              // pode ser null
        manutentorId,              // responsável atual = manutentor quando há
        JSON.stringify(checklist)  // jsonb
      ]
    );
    const chamadoId = crows[0]?.id;

    // 6) Atualiza status do agendamento
    await client.query(
      `UPDATE agendamentos_preventivos SET status = 'iniciado' WHERE id = $1`,
      [id]
    );

    await client.query("COMMIT");

    try { sseBroadcast?.({ topic: "agendamentos", action: "started", id, payload: { chamadoId } }); } catch {}
    res.json({ ok: true, chamadoId });
  } catch (e:any) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error(e);
    res.status(500).json({ error: String(e) });
  } finally {
    client.release();
  }
});

