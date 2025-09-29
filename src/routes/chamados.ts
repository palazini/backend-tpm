import { Router } from 'express';
import { requireRole } from '../middlewares/requireRole';
import { pool, withTx } from '../db';
import { CHAMADO_STATUS, normalizeChamadoStatus, isStatusAtivo } from '../utils/status';
import { sseBroadcast } from '../utils/sse';
import { z } from "zod";
import {
  CreateChamadoSchema,
  ConcluirChamadoSchema,
  PatchChecklistSchema,
  ObservacaoSchema,
  ChecklistItemSchema,
} from "@manutencao/shared";


export const chamadosRouter = Router();

// ---------- Chamados: lista com filtros + paginação ----------
chamadosRouter.get("/chamados", async (req, res) => {
  try {
    const status          = req.query.status as string | undefined;
    const tipo            = req.query.tipo   as string | undefined;
    const maquinaTag      = req.query.maquinaTag as string | undefined;
    const maquinaId       = req.query.maquinaId as string | undefined;
    const criadoPorEmail  = req.query.criadoPorEmail as string | undefined;
    const manutentorEmail = req.query.manutentorEmail as string | undefined;
    const from            = req.query.from as string | undefined;
    const to              = req.query.to   as string | undefined;

    const page = Math.max(parseInt(String(req.query.page ?? "1"), 10) || 1, 1);
    const pageSize = Math.min(Math.max(parseInt(String(req.query.pageSize ?? "20"), 10) || 20, 1), 100);
    const offset = (page - 1) * pageSize;

    const params: any[] = [];
    const where: string[] = [];

    // status (robusto p/ "Concluído", "Concluido", "concluido"...)
    let isConcluido = false;
    if (status) {
      const statusNorm = normalizeChamadoStatus(status);
      if (!statusNorm) {
        return res.status(400).json({ error: 'STATUS_INVALIDO' });
      }
      params.push(statusNorm);
      where.push(`LOWER(c.status) = LOWER($${params.length})`);
      isConcluido = (statusNorm === CHAMADO_STATUS.CONCLUIDO);
    }

    // tipo (case-insensitive)
    if (tipo) {
      params.push(tipo);
      where.push(`LOWER(c.tipo) = LOWER($${params.length})`);
    }

    if (maquinaTag) {
      params.push(maquinaTag);
      where.push(`m.tag = $${params.length}`);
    }

    if (maquinaId) {
      params.push(maquinaId);
      where.push(`c.maquina_id = $${params.length}`);
    }

    // e-mail de quem criou (case-insensitive)
    if (criadoPorEmail) {
      params.push(criadoPorEmail);
      where.push(`LOWER(u.email) = LOWER($${params.length})`);
    }

    // manutentor: cobre manutentor_id E colunas de atribuição por e-mail (se existirem)
    if (manutentorEmail) {
      params.push(manutentorEmail);
      const idx = params.length;
      where.push(`
        (
          LOWER(um.email) = LOWER($${idx})
          OR LOWER(COALESCE(c.atribuido_para_email, '')) = LOWER($${idx})
        )
      `);
    }

    // Período: se Concluído, filtra por concluido_em; senão por criado_em
    const dateCol = isConcluido ? "c.concluido_em" : "c.criado_em";
    if (from) {
      params.push(new Date(from).toISOString());
      where.push(`${dateCol} >= $${params.length}::timestamptz`);
    }
    if (to) {
      params.push(new Date(to).toISOString());
      where.push(`${dateCol} <= $${params.length}::timestamptz`);
    }

    const whereSql = where.length ? where.join(" AND ") : "1=1";

    // total
    const { rows: countRows } = await pool.query(
      `SELECT COUNT(*)::int AS total
         FROM public.chamados c
         JOIN public.maquinas  m  ON m.id  = c.maquina_id
         JOIN public.usuarios  u  ON u.id  = c.criado_por_id
         LEFT JOIN public.usuarios um ON um.id = c.manutentor_id
        WHERE ${whereSql}`,
      params
    );
    const total = countRows[0]?.total ?? 0;

    // itens
    const orderCol = isConcluido ? "c.concluido_em" : "c.criado_em";
    const params2 = [...params, pageSize, offset];
    const { rows: items } = await pool.query(
      `SELECT
         c.id,
         m.nome  AS maquina,
         c.tipo,
         c.status,
         c.causa,
         c.descricao,
         c.item,
         c.checklist_item_key AS "checklistItemKey",
         u.nome  AS criado_por,
         um.nome AS manutentor,
         to_char(c.criado_em,    'YYYY-MM-DD HH24:MI') AS criado_em,
         to_char(c.concluido_em, 'YYYY-MM-DD HH24:MI') AS concluido_em
       FROM public.chamados c
       JOIN public.maquinas  m  ON m.id  = c.maquina_id
       JOIN public.usuarios  u  ON u.id  = c.criado_por_id
       LEFT JOIN public.usuarios um ON um.id = c.manutentor_id
       WHERE ${whereSql}
       ORDER BY ${orderCol} DESC NULLS LAST
       LIMIT $${params2.length - 1} OFFSET $${params2.length}`,
      params2
    );

    res.json({ items, page, pageSize, total, hasNext: offset + items.length < total });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// ---------- Chamados: detalhe (patch 3 - versão completa) ----------
chamadosRouter.get("/chamados/:id", async (req, res) => {
  try {
    const id = String(req.params.id);

    // Detalhe do chamado + responsável atual
    const { rows } = await pool.query(
      `
      SELECT
        c.id,
        c.fs_id,
        m.nome AS maquina,
        c.tipo,
        c.status,
        CASE
          WHEN LOWER(c.status) LIKE 'abert%'       THEN 'aberto'
          WHEN LOWER(c.status) LIKE 'em andament%' THEN 'em_andamento'
          WHEN LOWER(c.status) LIKE 'conclu%'      THEN 'concluido'
          WHEN LOWER(c.status) LIKE 'cancel%'      THEN 'cancelado'
          ELSE 'aberto'
        END AS status_key,

        -- Texto / serviço
        c.descricao,
        c.problema_reportado,
        c.causa,
        COALESCE(c.solucao, c.servico_realizado) AS solucao,
        COALESCE(c.servico_realizado, c.solucao) AS servico_realizado,

        -- Datas
        to_char(c.criado_em,    'YYYY-MM-DD HH24:MI') AS criado_em,
        to_char(c.concluido_em, 'YYYY-MM-DD HH24:MI') AS concluido_em,

        -- quem criou
        c.criado_por_id,
        COALESCE(c.criado_por_nome, ucri.nome)   AS criado_por,
        ucri.email                                AS criado_por_email,

        -- Manutentor (quem atendeu)
         c.atendido_por_id,
         c.atendido_por_nome,
         c.atendido_por_email,
         to_char(c.atendido_em, 'YYYY-MM-DD HH24:MI') AS atendido_em,
         COALESCE(c.atendido_por_nome, umat.nome)     AS manutentor,
         COALESCE(c.atendido_por_email, umat.email)   AS manutentor_email,

        -- Atribuído (histórico da importação)
        c.atribuido_para_id,
        c.atribuido_para_nome,
        c.atribuido_para_email,

        -- Responsável atual (o que a UI mostra como Atribuído a)
        c.responsavel_atual_id,
        ru.nome   AS responsavel_atual_nome,
        ru.email  AS responsavel_atual_email,

        -- quem concluiu (novo)
        c.concluido_por_id,
        c.concluido_por_nome,
        c.concluido_por_email,

        -- Checklist sempre como JSONB
        CASE WHEN jsonb_typeof(c.checklist) = 'array' THEN c.checklist ELSE '[]'::jsonb END AS checklist,

        -- Metadados do checklist
        CASE WHEN c.tipo = 'preventiva' THEN 'preventiva' ELSE NULL END AS tipo_checklist,
        CASE WHEN c.tipo = 'preventiva' AND c.checklist IS NOT NULL
            THEN jsonb_array_length(c.checklist)
            ELSE NULL
        END AS qtd_itens,

        -- Aliases normalizados p/ o front
        COALESCE(c.responsavel_atual_id, c.atendido_por_id, c.atribuido_para_id)     AS manutentor_id_norm,
        COALESCE(ru.email,                umat.email,        c.atribuido_para_email) AS manutentor_email_norm,
        COALESCE(ru.nome,                 umat.nome,         c.atribuido_para_nome)  AS manutentor_nome_norm

      FROM public.chamados c
      JOIN public.maquinas  m   ON m.id  = c.maquina_id
      LEFT JOIN public.usuarios ucri ON ucri.id = c.criado_por_id
      LEFT JOIN public.usuarios umat ON umat.id = c.atendido_por_id
      LEFT JOIN public.usuarios ru   ON ru.id   = c.responsavel_atual_id
      WHERE c.id = $1
      LIMIT 1;
      `,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ error: "Chamado não encontrado." });
    }
    const chamado = rows[0];

    // Observações (texto sempre string; sem arrays SQL)
    const obs = await pool.query(
      `
      SELECT
        COALESCE(o.texto, o.mensagem, '')            AS texto,
        to_char(o.criado_em,'YYYY-MM-DD HH24:MI')     AS criado_em,
        COALESCE(o.autor_nome, u.nome, 'Sistema')     AS autor
      FROM public.chamado_observacoes o
      LEFT JOIN public.usuarios u ON u.id = o.autor_id
      WHERE o.chamado_id = $1
      ORDER BY o.criado_em ASC
      `,
      [id]
    );

    res.json({ ...chamado, observacoes: obs.rows });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// ---------- Chamados: observacoes ----------
chamadosRouter.post(
  "/chamados/:id/observacoes",
  requireRole(["operador", "manutentor", "gestor"]),
  async (req, res) => {
    try {
      const chamadoId = String(req.params.id);

      // ✅ validação + trim
      const parsed = ObservacaoSchema.safeParse(req.body ?? {});
      if (!parsed.success) {
        return res.status(400).json({
          error: "VALIDATION_ERROR",
          issues: parsed.error.issues.map(i => ({ path: i.path.join("."), message: i.message })),
        });
      }
      const { texto } = parsed.data;

      const user = req.user;
      const autorId   = user?.id ?? null;
      const autorNome = user?.name ? String(user.name).trim()
                        : user?.email ? String(user.email).trim()
                        : null;

      const { rows } = await pool.query(
        `INSERT INTO public.chamado_observacoes
           (chamado_id, autor_id, autor_nome, texto, criado_em)
         VALUES ($1, $2, $3, $4, NOW())
         RETURNING id, texto, criado_em`,
        [chamadoId, autorId, autorNome, texto]
      );

      const observacao = rows[0];

      const { rows: lista } = await pool.query(
        `SELECT
           COALESCE(o.texto, o.mensagem, '')          AS texto,
           to_char(o.criado_em, 'YYYY-MM-DD HH24:MI') AS criado_em,
           COALESCE(o.autor_nome, u.nome, 'Sistema')  AS autor
         FROM public.chamado_observacoes o
         LEFT JOIN public.usuarios u ON u.id = o.autor_id
        WHERE o.chamado_id = $1
        ORDER BY o.criado_em ASC`,
        [chamadoId]
      );

      const ultimaObservacao = lista[lista.length - 1] ?? observacao;

      try {
        sseBroadcast?.({
          topic: "chamados",
          action: "observacao-criada",
          id: chamadoId,
          payload: ultimaObservacao,
        });
      } catch {}

      return res.status(201).json({ ok: true, observacao: ultimaObservacao, observacoes: lista });
    } catch (error: any) {
      if (error?.code === "23503") {
        return res.status(404).json({ error: "CHAMADO_NAO_ENCONTRADO" });
      }
      console.error(error);
      return res.status(500).json({ error: String(error) });
    }
  }
);

// ---------- Chamados: atender ----------
chamadosRouter.post(
  "/chamados/:id/atender",
  requireRole(["manutentor"]),
  async (req, res) => {
    try {
      const user = req.user;
      if (!user?.id) {
        return res.status(401).json({ error: "USUARIO_NAO_CADASTRADO" });
      }

      const chamadoId = String(req.params.id);
      const atendenteId    = user.id;
      const atendenteEmail = user.email ? String(user.email).trim() : null;
      const atendenteNome  = user.name  ? String(user.name).trim()  : null;

      const resultado = await withTx(async (client) => {
        // trava a linha para transição segura
        const { rows } = await client.query(
          `SELECT status, tipo, manutentor_id, responsavel_atual_id, atendido_por_id, agendamento_id
             FROM public.chamados
            WHERE id = $1
            FOR UPDATE`,
          [chamadoId]
        );

        if (!rows.length) {
          return { notFound: true as const };
        }

        const atual = rows[0];
        const statusAtual = normalizeChamadoStatus(atual.status);

        // só permite atender a partir de "Aberto"
        if (statusAtual !== CHAMADO_STATUS.ABERTO) {
          return { conflict: String(atual.status) };
        }

        // Atualiza para "Em Andamento"
        const { rows: updated } = await client.query(
          `UPDATE public.chamados
              SET status               = $2,
                  -- não sobrescreve se já existir
                  manutentor_id        = COALESCE(manutentor_id, $3),
                  responsavel_atual_id = COALESCE(responsavel_atual_id, $3),

                  -- marca quem atendeu e quando (idempotente)
                  atendido_por_id      = COALESCE(atendido_por_id,    $3),
                  atendido_por_email   = COALESCE(atendido_por_email, $4),
                  atendido_por_nome    = COALESCE(atendido_por_nome,  $5),
                  atendido_em          = COALESCE(atendido_em,        NOW()),

                  atualizado_em        = NOW()
            WHERE id = $1
        RETURNING id, status, manutentor_id, responsavel_atual_id, atendido_por_id, atendido_em, agendamento_id`,
          [chamadoId, CHAMADO_STATUS.EM_ANDAMENTO, atendenteId, atendenteEmail, atendenteNome]
        );

        if (!updated.length) {
          return { conflict: String(atual.status) };
        }

        return { row: updated[0] };
      });

      if (resultado.notFound) {
        return res.status(404).json({ error: "CHAMADO_NAO_ENCONTRADO" });
      }
      if (resultado.conflict) {
        return res.status(409).json({ error: "STATE_CONFLICT", status: resultado.conflict });
      }

      try {
        sseBroadcast?.({ topic: "chamados", action: "updated", id: chamadoId });
      } catch {}

      // mantém o mesmo shape de retorno que você já usa
      return res.json({ ok: true, chamado: resultado.row });
    } catch (error) {
      console.error(error);
      return res.status(500).json({ error: String(error) });
    }
  }
);

// ---------- Chamados: concluir ----------
chamadosRouter.post(
  "/chamados/:id/concluir",
  requireRole(['manutentor']),
  async (req, res) => {
    try {
      const user = req.user;
      if (!user?.id) {
        return res.status(401).json({ error: "USUARIO_NAO_CADASTRADO" });
      }

      const chamadoId = String(req.params.id);

      // ✅ validação básica do body e normalização do checklist (se vier)
      const parsed = ConcluirChamadoSchema.safeParse(req.body ?? {});
      if (!parsed.success) {
        return res.status(400).json({
          error: "VALIDATION_ERROR",
          issues: parsed.error.issues.map((i) => ({ path: i.path.join("."), message: i.message })),
        });
      }
      const body = parsed.data;

      // lê status/tipo e vínculos para regras de permissão/transição
      const { rows } = await pool.query(
        `SELECT status,
                tipo,
                manutentor_id,
                responsavel_atual_id,
                atendido_por_id,
                agendamento_id,
                checklist
           FROM public.chamados
          WHERE id = $1`,
        [chamadoId]
      );

      if (!rows.length) {
        return res.status(404).json({ error: "CHAMADO_NAO_ENCONTRADO" });
      }

      const atual = rows[0];
      const statusAtual = normalizeChamadoStatus(atual.status);
      if (statusAtual !== CHAMADO_STATUS.EM_ANDAMENTO) {
        return res.status(409).json({ error: "STATE_CONFLICT", status: atual.status });
      }

      // Permissão: manutentor/resp/atendente podem concluir; gestor sempre pode
      const associados = [atual.manutentor_id, atual.responsavel_atual_id, atual.atendido_por_id]
        .filter(Boolean)
        .map((v) => String(v));
      if (user.role !== "gestor" && !associados.includes(String(user.id))) {
        return res.status(403).json({ error: "PERMISSAO_NEGADA" });
      }

      const tipoChamado = typeof atual.tipo === "string" ? atual.tipo.toLowerCase() : "";

      // ✅ Regras por tipo
      // - preventiva: checklist é obrigatório e com pelo menos 1 item já normalizado
      // - corretiva: causa e solucao obrigatórias; checklist opcional
      let checklistJson: string | null = null;

      if (tipoChamado === "preventiva") {
        if (!Array.isArray(body.checklist) || body.checklist.length === 0) {
          return res.status(400).json({ error: "CHECKLIST_OBRIGATORIO" });
        }
        checklistJson = JSON.stringify(body.checklist);
      } else if (tipoChamado === "corretiva") {
        const causaOk = typeof body.causa === "string" && body.causa.trim().length > 0;
        const solucaoOk = typeof body.solucao === "string" && body.solucao.trim().length > 0;
        if (!causaOk) return res.status(400).json({ error: "CAUSA_OBRIGATORIA" });
        if (!solucaoOk) return res.status(400).json({ error: "SOLUCAO_OBRIGATORIA" });

        if (Array.isArray(body.checklist) && body.checklist.length) {
          checklistJson = JSON.stringify(body.checklist);
        }
      } else {
        // se futuramente houver outros tipos, no mínimo aceite checklist se vier
        if (Array.isArray(body.checklist) && body.checklist.length) {
          checklistJson = JSON.stringify(body.checklist);
        }
      }

      const causaFinal =
        tipoChamado === "corretiva"
          ? (body.causa ?? "").trim()
          : (typeof body.causa === "string" ? body.causa.trim() : null) || null;

      const solucaoFinal =
        tipoChamado === "corretiva"
          ? (body.solucao ?? "").trim()
          : (typeof body.solucao === "string" ? body.solucao.trim() : null) || null;

      const chamadoAtualizado = await withTx(async (client) => {
        const paramsBase = {
          concluidorId: user.id,
          concluidorEmail: user.email ?? null,
          concluidorNome: user.name ?? null,
        };

        const qEnd = await client.query(
          `
          UPDATE public.chamados
             SET status               = $2,
                 concluido_em         = NOW(),
                 checklist            = COALESCE($3::jsonb, checklist),
                 causa                = COALESCE($4::text, causa),
                 solucao              = COALESCE($5::text, solucao),
                 servico_realizado    = COALESCE($5::text, servico_realizado),
                 concluido_por_id     = $6,
                 concluido_por_email  = $7,
                 concluido_por_nome   = $8,
                 atualizado_em        = NOW()
           WHERE id = $1
           RETURNING id, status, tipo, agendamento_id
          `,
          [
            chamadoId,
            CHAMADO_STATUS.CONCLUIDO,
            checklistJson,
            causaFinal,
            solucaoFinal,
            paramsBase.concluidorId,
            paramsBase.concluidorEmail,
            paramsBase.concluidorNome,
          ]
        );

        if (!qEnd.rowCount) return null;

        const row = qEnd.rows[0];

        if (row.agendamento_id) {
          await client.query(
            `UPDATE public.agendamentos_preventivos
                SET status = 'concluido',
                    concluido_em = NOW()
              WHERE id = $1`,
            [row.agendamento_id]
          );
        }

        return row;
      });

      if (!chamadoAtualizado) {
        return res.status(500).json({ error: "FALHA_ATUALIZAR_CHAMADO" });
      }

      try {
        sseBroadcast?.({ topic: "chamados", action: "updated", id: chamadoId });
      } catch {}

      return res.json({ ok: true, chamado: chamadoAtualizado });
    } catch (error) {
      console.error(error);
      return res.status(500).json({ error: String(error) });
    }
  }
);

chamadosRouter.patch(
  "/chamados/:id/checklist",
  requireRole(["manutentor", "gestor"]),
  async (req, res) => {
    try {
      const user = req.user;
      if (!user?.id) {
        return res.status(401).json({ error: "USUARIO_NAO_CADASTRADO" });
      }

      const parsed = PatchChecklistSchema.safeParse(req.body ?? {});
      if (!parsed.success) {
        return res.status(400).json({
          error: "VALIDATION_ERROR",
          issues: parsed.error.issues.map(i => ({ path: i.path.join("."), message: i.message })),
        });
      }
      const { checklist } = parsed.data;
      const chamadoId = String(req.params.id);

      // Carrega chamado para validar estado/permissão
      const { rows } = await pool.query(
        `SELECT status, manutentor_id, responsavel_atual_id, atendido_por_id
           FROM public.chamados
          WHERE id = $1
          LIMIT 1`,
        [chamadoId]
      );
      if (!rows.length) {
        return res.status(404).json({ error: "CHAMADO_NAO_ENCONTRADO" });
      }

      const atual = rows[0];
      const statusNorm = normalizeChamadoStatus(atual.status);
      if (statusNorm === CHAMADO_STATUS.CONCLUIDO || statusNorm === CHAMADO_STATUS.CANCELADO) {
        return res.status(409).json({ error: "INVALID_STATE", status: atual.status });
      }


      // manutentor/gestor: se não for gestor, precisa estar associado
      const associados = [atual.manutentor_id, atual.responsavel_atual_id, atual.atendido_por_id]
        .filter(Boolean)
        .map((v: any) => String(v));
      if (user.role !== "gestor" && !associados.includes(String(user.id))) {
        return res.status(403).json({ error: "PERMISSAO_NEGADA" });
      }

      await pool.query(
        `UPDATE public.chamados
            SET checklist = $2::jsonb,
                atualizado_em = NOW()
          WHERE id = $1`,
        [chamadoId, JSON.stringify(checklist)]
      );

      try { sseBroadcast?.({ topic: "chamados", action: "updated", id: chamadoId }); } catch {}

      return res.json({ ok: true });
    } catch (e: any) {
      console.error(e);
      return res.status(500).json({ error: String(e) });
    }
  }
);

// ---------- Chamados: criar ----------
/**
 * Body:
 * {
 *   "maquinaTag": "TCN-12"   // ou "maquinaNome": "TCN-12"
 *   "descricao": "texto...",
 *   "tipo": "corretiva" | "preventiva",      // padrão: "corretiva"
 *   "status": "Aberto" | "Em Andamento"      (padrão: "Aberto")
 *   "criadoPorEmail": "operador@local",
 *   "manutentorEmail": "manutentor@local"    // obrigatório se status = "Em Andamento"
 * }
 *
 * Regras:
 * - operador pode criar SOMENTE "Aberto" (sem manutentorEmail)
 * - manutentor/gestor podem criar "Aberto" ou "Em Andamento"
 */
chamadosRouter.post("/chamados", async (req, res) => {
  try {
    const user = (req as any).user as { role?: string; email?: string } | undefined;

    // ✅ validação de entrada (Zod)
    const parsed = CreateChamadoSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      return res.status(400).json({
        error: "VALIDATION_ERROR",
        issues: parsed.error.issues.map(i => ({ path: i.path.join("."), message: i.message })),
      });
    }

    const criadorEmail = user?.email;
    if (!criadorEmail) {
      return res.status(401).json({ error: "USUARIO_NAO_CADASTRADO" });
    }

    const {
      maquinaTag,
      maquinaNome,
      descricao,
      status = "Aberto",
      manutentorEmail,
      agendamentoId,
      checklistItems,
    } = parsed.data;

    // mantém sua normalização de status e regra de "ativo"
    const statusNorm = normalizeChamadoStatus(status) ?? CHAMADO_STATUS.ABERTO;
    if (!isStatusAtivo(statusNorm)) {
      return res.status(400).json({ error: "Status invalido para criacao." });
    }

    // tipo: igual ao seu (padrão corretiva; aceita preventiva)
    const tipo = String(req.body?.tipo || "corretiva").toLowerCase() === "preventiva"
      ? "preventiva"
      : "corretiva";

    // RBAC igual ao seu
    const role = user?.role ?? "gestor";
    if (role === "operador") {
      if (statusNorm !== CHAMADO_STATUS.ABERTO) {
        return res.status(403).json({ error: "Operador só pode criar chamados em 'Aberto'." });
      }
      if (manutentorEmail) {
        return res.status(403).json({ error: "Operador não pode atribuir manutentor ao criar." });
      }
    }

    // ------------------------------------------------------------------------------------------
    // 1) se vier de agendamento, carrega dados e checklist base
    // ------------------------------------------------------------------------------------------
    let maquinaIdFromAg: string | null = null;
    let checklistFromAg: any[] = [];

    if (agendamentoId) {
      const { rows: ags } = await pool.query(
        `SELECT a.id, a.maquina_id,
                COALESCE(a.itens_checklist, '[]'::jsonb) AS itens_checklist
           FROM public.agendamentos_preventivos a
          WHERE a.id = $1
          LIMIT 1`,
        [agendamentoId]
      );
      if (!ags.length) {
        return res.status(400).json({ error: "agendamentoId inválido." });
      }
      maquinaIdFromAg = ags[0].maquina_id;

      if (Array.isArray(ags[0].itens_checklist)) {
        checklistFromAg = ags[0].itens_checklist.map((t: any) => ({
          item: String(t),
          resposta: "sim",
        }));
      }
    }

    // ------------------------------------------------------------------------------------------
    // 2) checklist direto do body (strings) tem prioridade; senão, usa o do agendamento
    // ------------------------------------------------------------------------------------------
    let checklistFinal: any[] = [];
    if (Array.isArray(checklistItems) && checklistItems.length) {
      checklistFinal = checklistItems.map((t: any) => ({
        item: String(t),
        resposta: "sim",
      }));
    } else if (checklistFromAg.length) {
      checklistFinal = checklistFromAg;
    }

    // ------------------------------------------------------------------------------------------
    // 3) resolve máquina: por agendamento OU por tag/nome
    // ------------------------------------------------------------------------------------------
    if (!maquinaIdFromAg && !maquinaTag && !maquinaNome) {
      return res.status(400).json({ error: "Informe maquinaTag ou maquinaNome (ou agendamentoId)." });
    }

    // ids de usuários (criador e, se necessário, manutentor)
    const { rows: uCriador } = await pool.query(
      `SELECT id FROM public.usuarios WHERE LOWER(email) = LOWER($1) LIMIT 1`,
      [criadorEmail]
    );
    if (!uCriador.length) {
      return res.status(400).json({ error: "criadoPorEmail inválido (header)" });
    }

    let manutentorId: string | null = null;
    if (statusNorm === CHAMADO_STATUS.EM_ANDAMENTO && manutentorEmail) {
      const { rows: uMant } = await pool.query(
        `SELECT id FROM public.usuarios WHERE LOWER(email) = LOWER($1) LIMIT 1`,
        [manutentorEmail]
      );
      if (!uMant.length) {
        return res.status(400).json({ error: "manutentorEmail inválido." });
      }
      manutentorId = uMant[0].id;
    }

    // máquina via agendamento OU (tag|nome)
    let maquinaId: string | null = maquinaIdFromAg;
    if (!maquinaId) {
      const { rows: maq } = await pool.query(
        `SELECT id FROM public.maquinas
          WHERE ($1::text IS NOT NULL AND tag = $1)
             OR ($2::text IS NOT NULL AND nome = $2)
          LIMIT 1`,
        [maquinaTag ?? null, maquinaNome ?? null]
      );
      if (!maq.length) {
        return res.status(400).json({ error: "Máquina não encontrada (tag/nome)." });
      }
      maquinaId = maq[0].id;
    }

    // ------------------------------------------------------------------------------------------
    // 4) INSERT do chamado (com checklist jsonb). responsável = manutentor se "Em Andamento"
    // ------------------------------------------------------------------------------------------
    const { rows: created } = await pool.query(
      `INSERT INTO public.chamados
         (maquina_id, tipo, status, descricao,
          criado_por_id, manutentor_id, responsavel_atual_id,
          checklist)
       VALUES ($1, $2, $3, $4,
               $5, $6, $7,
               $8::jsonb)
       RETURNING id`,
      [
        maquinaId,
        tipo,
        statusNorm,
        String(descricao).trim(),
        uCriador[0].id,
        manutentorId,
        manutentorId,
        JSON.stringify(checklistFinal),
      ]
    );

    const chamadoId = created[0].id;

    if (agendamentoId) {
      await pool.query(
        `UPDATE public.agendamentos_preventivos
            SET status = 'iniciado', iniciado_em = NOW()
          WHERE id = $1`,
        [agendamentoId]
      );
    }

    // retorna no mesmo formato que sua lista já usa
    const { rows } = await pool.query(
      `SELECT
         c.id,
         m.nome  AS maquina,
         c.tipo,
         c.status,
         CASE
           WHEN LOWER(c.status) LIKE 'abert%'       THEN 'aberto'
           WHEN LOWER(c.status) LIKE 'em andament%' THEN 'em_andamento'
           WHEN LOWER(c.status) LIKE 'conclu%'      THEN 'concluido'
           WHEN LOWER(c.status) LIKE 'cancel%'      THEN 'cancelado'
           ELSE 'aberto'
         END AS status_key,
         c.descricao,
         u.nome  AS criado_por,
         um.nome AS manutentor,
         to_char(c.criado_em, 'YYYY-MM-DD HH24:MI') AS criado_em
       FROM public.chamados c
       JOIN public.maquinas  m  ON m.id  = c.maquina_id
       JOIN public.usuarios  u  ON u.id  = c.criado_por_id
       LEFT JOIN public.usuarios um ON um.id = c.manutentor_id
       WHERE c.id = $1`,
      [chamadoId]
    );

    try { sseBroadcast?.({ topic: "chamados", action: "created", id: chamadoId }); } catch {}

    return res.status(201).json(rows[0]);
  } catch (e:any) {
    console.error(e);
    return res.status(500).json({ error: String(e) });
  }
});

// ---------- Chamados: atualizar status ----------
/**
 * Body:
 * {
 *   "status": "Aberto" | "Em Andamento" | "Concluído",
 *   "manutentorEmail": "manutentor@local"  // obrigatório se status = "Em Andamento"
 * }
 *
 * Regras:
 * - "Em Andamento": manutentor/gestor
 * - "Concluído":    manutentor/gestor
 * - "Aberto":       gestor
 */
chamadosRouter.patch("/chamados/:id", async (req, res) => {
  try {
    const user = (req as any).user as { role?: string; email?: string } | undefined;
    const role = user?.role ?? "gestor"; // ambiente dev: default libera

    const id = String(req.params.id);
    const manutentorEmail = req.body?.manutentorEmail as string | undefined;

    const rawStatus = req.body?.status as string | undefined;
    const statusNorm = normalizeChamadoStatus(rawStatus);
    if (!statusNorm) return res.status(400).json({ error: "STATUS_INVALIDO" });

    const isEmAndamento = statusNorm === CHAMADO_STATUS.EM_ANDAMENTO;
    const isConcluido   = statusNorm === CHAMADO_STATUS.CONCLUIDO;
    const isAberto      = statusNorm === CHAMADO_STATUS.ABERTO;

    if (isEmAndamento && !(role === "manutentor" || role === "gestor")) {
      return res.status(403).json({ error: "Apenas manutentor/gestor podem mover para 'Em Andamento'." });
    }
    if (isEmAndamento && !manutentorEmail) {
      return res.status(400).json({ error: "manutentorEmail é obrigatório quando status = 'Em Andamento'." });
    }
    if (isConcluido && !(role === "manutentor" || role === "gestor")) {
      return res.status(403).json({ error: "Apenas manutentor/gestor podem concluir." });
    }
    if (isAberto && role !== "gestor") {
      return res.status(403).json({ error: "Apenas gestor pode reabrir para 'Aberto'." });
    }

    const sql = `
      WITH mt AS (SELECT id FROM usuarios WHERE email = $2 LIMIT 1)
      UPDATE chamados c
      SET
        status = $1,
        manutentor_id = CASE
          WHEN $1 = 'Em Andamento' THEN (SELECT id FROM mt)
          WHEN $1 = 'Aberto' THEN NULL
          ELSE c.manutentor_id
        END,
        responsavel_atual_id = CASE
          WHEN $1 = 'Em Andamento' THEN (SELECT id FROM mt)
          WHEN $1 = 'Aberto' THEN NULL
          ELSE c.responsavel_atual_id
        END,
        concluido_em = CASE
          WHEN $1 = 'Concluido' THEN NOW()
          WHEN $1 = 'Aberto' THEN NULL
          ELSE c.concluido_em
        END,
        concluido_por_id = CASE WHEN $1 = 'Concluido' THEN $3 ELSE c.concluido_por_id END,
        concluido_por_email = CASE WHEN $1 = 'Concluido' THEN $4 ELSE c.concluido_por_email END,
        concluido_por_nome  = CASE WHEN $1 = 'Concluido' THEN $5 ELSE c.concluido_por_nome  END,
        atualizado_em = NOW()
      WHERE c.id = $6
      RETURNING c.id;
    `;

    const upd = await pool.query(sql, [
      statusNorm,
      manutentorEmail ?? null,
      (req as any)?.user?.id ?? null,
      (req as any)?.user?.email ?? null,
      (req as any)?.user?.name ?? null,
      id,
    ]);
    if (upd.rowCount === 0) {
      return res.status(404).json({ error: "Chamado não encontrado ou manutentor inexistente." });
    }

    const { rows } = await pool.query(
      `SELECT
         c.id, m.nome AS maquina, c.tipo, c.status, c.descricao,
         u.nome AS criado_por, um.nome AS manutentor,
         to_char(c.criado_em, 'YYYY-MM-DD HH24:MI') AS criado_em
       FROM chamados c
       JOIN maquinas  m  ON m.id  = c.maquina_id
       JOIN usuarios  u  ON u.id  = c.criado_por_id
       LEFT JOIN usuarios um ON um.id = c.manutentor_id
       WHERE c.id = $1`,
      [id]
    );

    // SSE broadcast
    sseBroadcast({ topic: "chamados", action: "updated", id });

    res.json(rows[0]);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});


