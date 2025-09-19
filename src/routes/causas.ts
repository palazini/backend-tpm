import { Router } from 'express';
import { pool } from '../db';

export const causasRouter = Router();

causasRouter.get('/causas', async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT id, nome
         FROM causas_raiz
        ORDER BY nome ASC`
    );
    // 👇 devolve o array direto
    res.json(rows);
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// POST /causas  (somente gestor)
causasRouter.post('/causas', async (req, res) => {
  try {
    const auth = (req as any).user || {};
    if (auth.role !== 'gestor') return res.status(403).json({ error: 'Somente gestor.' });

    const nome = String(req.body?.nome || '').trim();
    if (!nome) return res.status(400).json({ error: 'Nome é obrigatório.' });

    const ins = await pool.query(
      `INSERT INTO causas_raiz (nome)
       VALUES ($1)
       ON CONFLICT (nome) DO UPDATE SET nome = EXCLUDED.nome
       RETURNING id, nome`,
      [nome]
    );
    res.status(201).json(ins.rows[0]);
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// DELETE /causas/:id  (somente gestor)
causasRouter.delete('/causas/:id', async (req, res) => {
  try {
    const auth = (req as any).user || {};
    if (auth.role !== 'gestor') return res.status(403).json({ error: 'Somente gestor.' });

    const id = String(req.params.id);
    const del = await pool.query(`DELETE FROM causas_raiz WHERE id = $1`, [id]);
    if (!del.rowCount) return res.status(404).json({ error: 'Causa não encontrada.' });

    res.json({ ok: true });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// GET /usuarios
