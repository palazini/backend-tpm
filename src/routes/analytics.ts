import { Router } from 'express';
import { pool } from '../db';

export const analyticsRouter = Router();

type ParetoRow = { causa: string; chamados: number };

analyticsRouter.get('/analytics/pareto-causas', async (req, res) => {
  try {
    const from = req.query.from ? new Date(String(req.query.from)) : null;
    const to = req.query.to ? new Date(String(req.query.to)) : null;

    const now = new Date();
    const defaultTo = new Date(now);
    const defaultFrom = new Date(now);
    defaultFrom.setDate(defaultFrom.getDate() - 90);

    const rangeFrom = (from ?? defaultFrom).toISOString();
    const rangeTo = (to ?? defaultTo).toISOString();

    const { rows } = await pool.query<ParetoRow>(
      `WITH base AS (
         SELECT btrim(c.causa) AS causa_raw
           FROM chamados c
          WHERE LOWER(c.status) LIKE 'conclu%'
            AND COALESCE(c.concluido_em, c.criado_em) >= $1::timestamptz
            AND COALESCE(c.concluido_em, c.criado_em) <  $2::timestamptz
            AND c.causa IS NOT NULL
            AND btrim(c.causa) <> ''
       ),
       norm AS (
         SELECT CASE
           WHEN unaccent(lower(causa_raw)) IN ('mecanica') THEN 'Mecânica'
           WHEN unaccent(lower(causa_raw)) IN ('eletrica') THEN 'Elétrica'
           WHEN unaccent(lower(causa_raw)) IN ('vazamento','vazamentos') THEN 'Vazamento'
           WHEN unaccent(lower(causa_raw)) IN ('falta de lubrificacao','falta lubrificacao','falta de lubrificação') THEN 'Falta de Lubrificação'
           ELSE initcap(causa_raw)
         END AS causa
         FROM base
       )
       SELECT causa, COUNT(*)::int AS chamados
         FROM norm
        GROUP BY causa
        ORDER BY chamados DESC, causa;`,
      [rangeFrom, rangeTo]
    );

    const total = rows.reduce((sum: number, row) => sum + Number(row.chamados || 0), 0);
    let acumulado = 0;

    const items = rows.map((row) => {
      const pct = total ? (row.chamados / total) * 100 : 0;
      acumulado += pct;
      return {
        causa: row.causa,
        chamados: row.chamados,
        pct: Number(pct.toFixed(2)),
        pctAcum: Number(Math.min(acumulado, 100).toFixed(2)),
      };
    });

    res.json({
      total,
      items,
      from: rangeFrom.slice(0, 10),
      to: rangeTo.slice(0, 10),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: String(error) });
  }
});
