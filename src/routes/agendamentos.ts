import { Router } from 'express';
import { pool, withTx } from '../db';
import { slugify } from '../utils/slug';
import { sseBroadcast } from '../utils/sse';
import { AGENDAMENTO_STATUS, normalizeAgendamentoStatus, CHAMADO_STATUS } from '../utils/status';

type ChecklistItem = { texto: string; key: string };

const AGENDAMENTO_SELECT = `
  SELECT
    a.id,
    a.maquina_id,
    m.nome AS maquina_nome,
    a.descricao,
    a.itens_checklist,
    a.original_start,
    a.original_end,
    a.start_ts,
    a.end_ts,
    a.status,
    a.criado_em,
    a.concluido_em,
    (a.status = '${AGENDAMENTO_STATUS.CONCLUIDO}' AND a.concluido_em > a.end_ts) AS atrasado
  FROM agendamentos_preventivos a
  JOIN maquinas m ON m.id = a.maquina_id
`;

function toChecklistItem(value: unknown, index: number): ChecklistItem | null {
  const candidate = value as { texto?: unknown; key?: unknown } | undefined;
  const texto = String(candidate?.texto ?? value ?? '').trim();
  if (!texto) return null;

  const keySource = candidate?.key ?? null;
  const key = keySource ? slugify(String(keySource)) : slugify(texto || String(index));
  return { texto, key };
}

function normalizeChecklist(raw: unknown): ChecklistItem[] {
  if (Array.isArray(raw)) {
    return raw.map((item, index) => toChecklistItem(item, index)).filter(Boolean) as ChecklistItem[];
  }

  if (typeof raw === 'string') {
    let parsed: unknown;

    try {
      parsed = JSON.parse(raw);
    } catch {
      parsed = undefined;
    }

    if (Array.isArray(parsed)) {
      return normalizeChecklist(parsed);
    }

    const lines = raw
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);

    return lines.map((line, index) => toChecklistItem(line, index)).filter(Boolean) as ChecklistItem[];
  }

  return [];
}

export const agendamentosRouter = Router();

/* =========================
   2.2 - AGENDAMENTOS PREVENTIVOS
   ========================= */

// Listar agendamentos (por janela e/ou limite)
agendamentosRouter.get('/agendamentos', async (req, res) => {
  try {
    const { from, to, limit, order } = req.query as {
      from?: string;
      to?: string;
      limit?: string;
      order?: string;
    };

    const params: any[] = [];
    const where: string[] = [];

    if (from) {
      params.push(from);
      where.push(`a.start_ts >= $${params.length}`);
    }

    if (to) {
      params.push(to);
      where.push(`a.end_ts <= $${params.length}`);
    }

    const whereSql = where.length ? where.join(' AND ') : '1=1';
    const limitNumber = Math.min(Math.max(parseInt(limit ?? '0', 10) || 0, 0), 500);
    const orderSql = order === 'recent' ? 'a.criado_em DESC' : 'a.start_ts ASC';

    const { rows } = await pool.query(
      `
        ${AGENDAMENTO_SELECT}
        WHERE ${whereSql}
        ORDER BY ${orderSql}
        ${limitNumber > 0 ? `LIMIT ${limitNumber}` : ''}
      `,
      params,
    );

    res.json(rows);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: String(error) });
  }
});

// Criar agendamento
agendamentosRouter.post('/agendamentos', async (req, res) => {
  try {
    const { maquinaId, descricao, itensChecklist, start, end } = req.body ?? {};
    if (!maquinaId || !descricao || !start || !end) {
      return res.status(400).json({
        error: 'Campos obrigatorios: maquinaId, descricao, start, end.',
      });
    }

    const itens = normalizeChecklist(itensChecklist);

    const { rows } = await pool.query(
      `INSERT INTO agendamentos_preventivos
         (maquina_id, descricao, itens_checklist, original_start, original_end, start_ts, end_ts, status)
       VALUES ($1, $2, $3::jsonb, $4, $5, $4, $5, $6)
       RETURNING id`,
      [
        maquinaId,
        String(descricao).trim(),
        JSON.stringify(itens),
        start,
        end,
        AGENDAMENTO_STATUS.AGENDADO,
      ],
    );

    const id = rows[0].id;

    const { rows: selected } = await pool.query(
      `${AGENDAMENTO_SELECT}
       WHERE a.id = $1`,
      [id],
    );

    try {
      sseBroadcast?.({ topic: 'agendamentos', action: 'created', id });
    } catch {}

    res.status(201).json(selected[0]);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: String(error) });
  }
});

// Atualizar (reagendar) - gestor
agendamentosRouter.patch('/agendamentos/:id', async (req, res) => {
  try {
    const role = req.user?.role ?? 'operador';
    if (role !== 'gestor') {
      return res.status(403).json({ error: 'Somente gestor pode reagendar.' });
    }

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
      sets.push(`end_ts = $${params.length}`);
    }

    if (status !== undefined) {
      const normalizedStatus = normalizeAgendamentoStatus(String(status));
      if (!normalizedStatus) {
        return res.status(400).json({ error: 'Status invalido.' });
      }

      params.push(normalizedStatus);
      sets.push(`status = $${params.length}`);

      if (normalizedStatus === AGENDAMENTO_STATUS.CONCLUIDO) {
        sets.push('concluido_em = NOW()');
      } else {
        sets.push('concluido_em = NULL');
      }
    }

    if (!sets.length) {
      return res.status(400).json({ error: 'Nada para atualizar.' });
    }

    params.push(id);

    const { rowCount } = await pool.query(
      `UPDATE agendamentos_preventivos SET ${sets.join(', ')} WHERE id = $${params.length}`,
      params,
    );

    if (!rowCount) {
      return res.status(404).json({ error: 'Agendamento nao encontrado.' });
    }

    try {
      sseBroadcast?.({ topic: 'agendamentos', action: 'updated', id });
    } catch {}

    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: String(error) });
  }
});

// Deletar - gestor
agendamentosRouter.delete('/agendamentos/:id', async (req, res) => {
  try {
    const role = req.user?.role ?? 'operador';
    if (role !== 'gestor') {
      return res.status(403).json({ error: 'Somente gestor pode deletar.' });
    }

    const id = String(req.params.id);
    const { rowCount } = await pool.query(
      `DELETE FROM agendamentos_preventivos WHERE id = $1`,
      [id],
    );

    if (!rowCount) {
      return res.status(404).json({ error: 'Agendamento nao encontrado.' });
    }

    try {
      sseBroadcast?.({ topic: 'agendamentos', action: 'deleted', id });
    } catch {}

    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: String(error) });
  }
});

// Iniciar manutencao (gera chamado preventivo + muda status)
agendamentosRouter.post('/agendamentos/:id/iniciar', async (req, res) => {
  try {
    const user = req.user;
    const role = user?.role ?? 'operador';
    if (!['manutentor', 'gestor'].includes(role)) {
      return res.status(403).json({ error: 'Apenas manutentor/gestor podem iniciar manutencao.' });
    }

    const id = String(req.params.id);
    const criadoPorEmail = String(req.body?.criadoPorEmail || user?.email || '').trim().toLowerCase();
    const manutentorEmail = req.body?.manutentorEmail
      ? String(req.body.manutentorEmail).trim().toLowerCase()
      : null;

    if (!criadoPorEmail) {
      return res.status(400).json({ error: 'Informe criadoPorEmail.' });
    }

    const fail = (statusCode: number, message: string): never => {
      const err = new Error(message) as Error & { status?: number };
      err.status = statusCode;
      throw err;
    };

    const { chamadoId } = await withTx(async (client) => {
      const { rows } = await client.query(
        `SELECT a.id,
                a.maquina_id,
                a.descricao,
                a.status,
                COALESCE(a.itens_checklist, '[]'::jsonb) AS itens_checklist,
                m.nome AS maquina_nome
           FROM agendamentos_preventivos a
           JOIN maquinas m ON m.id = a.maquina_id
          WHERE a.id = $1
          FOR UPDATE`,
        [id],
      );

      const agendamento = rows[0];
      if (!agendamento) {
        fail(404, 'Agendamento nao encontrado.');
      }

      const statusAtual = normalizeAgendamentoStatus(agendamento.status);
      if (statusAtual !== AGENDAMENTO_STATUS.AGENDADO) {
        fail(409, 'Agendamento nao esta disponivel para inicio.');
      }

      const { rows: criadorRows } = await client.query(
        `SELECT id FROM usuarios WHERE LOWER(email) = LOWER($1) LIMIT 1`,
        [criadoPorEmail],
      );

      if (!criadorRows.length) {
        fail(400, 'Usuario (criadoPorEmail) nao existe em usuarios.');
      }

      const criadoPorId: string = criadorRows[0].id;

      let manutentorId: string | null = null;
      if (manutentorEmail) {
        const { rows: manutentorRows } = await client.query(
          `SELECT id FROM usuarios WHERE LOWER(email) = LOWER($1) LIMIT 1`,
          [manutentorEmail],
        );

        if (!manutentorRows.length) {
          fail(400, 'Manutentor informado nao existe em usuarios.');
        }

        manutentorId = manutentorRows[0].id;
      }

      const checklist = normalizeChecklist(agendamento.itens_checklist);
      const statusInicial = manutentorId ? CHAMADO_STATUS.EM_ANDAMENTO : CHAMADO_STATUS.ABERTO;
      const descricaoChamado = `Preventiva: ${agendamento.descricao || agendamento.maquina_nome}`.trim();

      const { rows: chamados } = await client.query(
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
          agendamento.maquina_id,
          statusInicial,
          descricaoChamado,
          criadoPorId,
          manutentorId,
          manutentorId,
          JSON.stringify(checklist),
        ],
      );

      const chamadoId = chamados[0]?.id;

      await client.query(
        `UPDATE agendamentos_preventivos SET status = $2 WHERE id = $1`,
        [id, AGENDAMENTO_STATUS.INICIADO],
      );

      return { chamadoId };
    });

    try {
      sseBroadcast?.({ topic: 'agendamentos', action: 'started', id, payload: { chamadoId } });
    } catch {}

    res.json({ ok: true, chamadoId });
  } catch (error: any) {
    if (error?.status) {
      return res.status(error.status).json({ error: error.message });
    }

    console.error(error);
    res.status(500).json({ error: String(error) });
  }
});
