import { Router } from 'express';
import { pool, withTx } from '../db';
import { sseBroadcast } from '../utils/sse';

export const pecasRouter = Router();

pecasRouter.post('/pecas', async (req, res) => {
  try {
    const auth = (req as any).user || {};
    if (auth.role !== 'gestor') return res.status(403).json({ error: 'Somente gestor.' });

    const {
      codigo,
      nome,
      categoria = null,
      estoqueMinimo = 0,
      localizacao = null,
      estoqueAtual = 0, // opcional (normalmente comeÃ§amos em 0)
    } = req.body || {};

    if (!codigo || !nome) return res.status(400).json({ error: 'Informe cÃ³digo e nome.' });
    if (estoqueMinimo < 0 || estoqueAtual < 0) return res.status(400).json({ error: 'Estoque nÃ£o pode ser negativo.' });

    const insert = await pool.query(
      `INSERT INTO pecas (codigo, nome, categoria, estoque_minimo, localizacao, estoque_atual)
       VALUES ($1,$2,$3,$4,$5,$6)
       RETURNING id, codigo, nome, categoria,
                 estoque_atual AS "estoqueAtual",
                 estoque_minimo AS "estoqueMinimo",
                 localizacao`,
      [codigo, nome, categoria, estoqueMinimo, localizacao, estoqueAtual]
    );

    // opcional: notificar SSE
    sseBroadcast({ topic: 'pecas', action: 'created', id: insert.rows[0].id });

    res.status(201).json(insert.rows[0]);
  } catch (e: any) {
    if (String(e?.message || '').includes('pecas_codigo_key')) {
      return res.status(409).json({ error: 'JÃ¡ existe uma peÃ§a com esse cÃ³digo.' });
    }
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// ATUALIZAR PEÃ‡A (somente gestor)
pecasRouter.put('/pecas/:id', async (req, res) => {
  try {
    const auth = (req as any).user || {};
    if (auth.role !== 'gestor') return res.status(403).json({ error: 'Somente gestor.' });

    const id = String(req.params.id);
    const {
      codigo,
      nome,
      categoria = null,
      estoqueMinimo = 0,
      localizacao = null
    } = req.body || {};

    if (!codigo || !nome) return res.status(400).json({ error: 'Informe cÃ³digo e nome.' });
    if (estoqueMinimo < 0) return res.status(400).json({ error: 'Estoque mÃ­nimo invÃ¡lido.' });

    const upd = await pool.query(
      `UPDATE pecas
          SET codigo=$2, nome=$3, categoria=$4, estoque_minimo=$5, localizacao=$6
        WHERE id=$1
      RETURNING id, codigo, nome, categoria,
                estoque_atual AS "estoqueAtual",
                estoque_minimo AS "estoqueMinimo",
                localizacao`,
      [id, codigo, nome, categoria, estoqueMinimo, localizacao]
    );

    if (!upd.rowCount) return res.status(404).json({ error: 'PeÃ§a nÃ£o encontrada.' });

    sseBroadcast({ topic: 'pecas', action: 'updated', id });
    res.json(upd.rows[0]);
  } catch (e: any) {
    if (String(e?.message || '').includes('pecas_codigo_key')) {
      return res.status(409).json({ error: 'JÃ¡ existe uma peÃ§a com esse cÃ³digo.' });
    }
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// Lista de peÃ§as (para a EstoquePage)
pecasRouter.get('/pecas', async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT
         id,
         codigo,
         nome,
         categoria,
         estoque_atual  AS "estoqueAtual",
         estoque_minimo AS "estoqueMinimo",
         localizacao
       FROM pecas
       ORDER BY codigo ASC`
    );
    res.json({ items: rows });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// DELETE /pecas/:id  (somente gestor)
pecasRouter.delete('/pecas/:id', async (req, res) => {
  try {
    const auth = (req as any).user || {};
    if (auth.role !== 'gestor') {
      return res.status(403).json({ error: 'Somente gestor.' });
    }

    const id = String(req.params.id);
    const r = await pool.query('DELETE FROM pecas WHERE id = $1', [id]);
    if (r.rowCount === 0) {
      return res.status(404).json({ error: 'PeÃ§a nÃ£o encontrada.' });
    }

    sseBroadcast({ topic: 'pecas', action: 'deleted', id });
    res.json({ ok: true });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// Lista de chamados com filtros opcionais (tipo, status, PerÃ­odo e mÃ¡quina)

pecasRouter.post('/pecas/:id/movimentacoes', async (req, res) => {
  try {
    const auth = (req as any).user || {};
    if (!['gestor','manutentor'].includes(auth.role)) {
      return res.status(403).json({ error: 'Somente gestor/manutentor.' });
    }
    const pecaId = String(req.params.id);
    const { tipo, quantidade, descricao } = req.body || {};
    const q = Number(quantidade);
    if (!['entrada','saida'].includes(String(tipo)) || !Number.isFinite(q) || q <= 0) {
      return res.status(400).json({ error: 'Dados invÃ¡lidos.' });
    }
    const delta = tipo === 'entrada' ? q : -q;

    const { movimentacaoId, peca } = await withTx(async (client) => {
      const mov = await client.query(
        `INSERT INTO movimentacoes (peca_id, tipo, quantidade, descricao, usuario_email)
         VALUES ($1,$2,$3,$4,$5) RETURNING id`,
        [pecaId, tipo, q, descricao || null, auth.email || null]
      );
      if (!mov.rowCount) throw new Error('Falha ao registrar movimentaÃ§Ã£o.');

      const upd = await client.query(
        `UPDATE pecas SET estoque_atual = estoque_atual + $2, atualizado_em = NOW()
         WHERE id = $1 RETURNING id, codigo, nome, estoque_atual`,
        [pecaId, delta]
      );
      if (!upd.rowCount) throw new Error('PeÃ§a nÃ£o encontrada.');

      return { movimentacaoId: mov.rows[0].id, peca: upd.rows[0] };
    });

    res.json({ ok: true, movimentacaoId, peca });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});



