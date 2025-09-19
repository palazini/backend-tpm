import { Router } from 'express';
import bcrypt from 'bcryptjs';
import { pool } from '../db';
import { roleToFuncao } from '../utils/roles';

export const usuariosRouter = Router();

usuariosRouter.get('/usuarios', async (req, res) => {
  try {
    // normaliza role e tolera "all:" etc.
    const rawRole = (req.query.role as string | undefined) ?? '';
    const role = rawRole.trim().toLowerCase().replace(/[:;,.]+$/, '');

    const includeInactive =
      String(req.query.includeInactive || 'false').toLowerCase() === 'true';

    const where: string[] = [];
    const params: any[] = [];

    // role = 'all' | 'todos' => sem filtro por papel
    if (role && role !== 'all' && role !== 'todos') {
      params.push(role);
      where.push(`LOWER(role) = LOWER($${params.length})`);
    }

    // por padrÃ£o, sÃ³ ativos
    if (!includeInactive) {
      where.push(`ativo = true`);
    }

    const whereSQL = where.length ? `WHERE ${where.join(' AND ')}` : '';

    const { rows } = await pool.query(
      `SELECT
         id,
         nome,
         usuario,
         email,
         role,
         COALESCE(
           funcao,
           CASE
             WHEN LOWER(role) = 'gestor'     THEN 'Gestor'
             WHEN LOWER(role) = 'manutentor' THEN 'TÃ©cnico EletromecÃ¢nico'
             ELSE 'Operador de CNC'
           END
         ) AS funcao,
         ativo
       FROM usuarios
       ${whereSQL}
       ORDER BY nome ASC`,
       params
    );

    res.json({ items: rows });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});


// POST /usuarios (gestor)
usuariosRouter.post('/usuarios', async (req, res) => {
  try {
    const auth = (req as any).user || {};
    if (auth.role !== 'gestor') return res.status(403).json({ error: 'Somente gestor.' });

    let { nome, usuario, email, role, funcao, senha } = req.body || {};
    nome    = String(nome || '').trim();
    usuario = String(usuario || '').trim().toLowerCase();
    email   = String(email || '').trim().toLowerCase();
    role    = String(role || '').trim().toLowerCase();
    funcao  = String(funcao || '').trim();

    if (!nome || !usuario || !email || !role) {
      return res.status(400).json({ error: 'Campos obrigatÃ³rios: nome, usuario, email, role.' });
    }

    // FunÃ§Ã£o padrÃ£o baseada no role (se nÃ£o vier)
    if (!funcao) {
      funcao = roleToFuncao(role);
    }

    // senha_hash (opcional)
    let senha_hash: string | null = null;
    if (typeof senha === 'string' && senha.trim().length >= 6) {
      senha_hash = await bcrypt.hash(senha.trim(), 10);
    }

    // InserÃ§Ã£o
    const { rows } = await pool.query(
      `INSERT INTO usuarios (nome, usuario, email, role, funcao, senha_hash)
       VALUES ($1,$2,$3,$4,$5,$6)
       RETURNING id, nome, usuario, email, role, funcao`,
      [nome, usuario, email, role, funcao, senha_hash]
    );

    res.status(201).json(rows[0]);
  } catch (e: any) {
    // conflitos de unique (usuario/email)
    if (String(e?.code) === '23505') {
      return res.status(409).json({ error: 'UsuÃ¡rio ou e-mail jÃ¡ existente.' });
    }
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// PUT /usuarios/:id (gestor)
usuariosRouter.put('/usuarios/:id', async (req, res) => {
  try {
    const auth = (req as any).user || {};
    if (auth.role !== 'gestor') return res.status(403).json({ error: 'Somente gestor.' });

    const id = String(req.params.id);

    let { nome, usuario, email, role, funcao, senha } = req.body || {};
    nome    = nome    !== undefined ? String(nome).trim()    : undefined;
    usuario = usuario !== undefined ? String(usuario).trim().toLowerCase() : undefined;
    email   = email   !== undefined ? String(email).trim().toLowerCase()   : undefined;
    role    = role    !== undefined ? String(role).trim().toLowerCase()    : undefined;
    funcao  = funcao  !== undefined ? String(funcao).trim()  : undefined;

    // Monta SET dinÃ¢mico
    const sets: string[] = [];
    const params: any[] = [];
    const add = (sql: string, v: any) => { params.push(v); sets.push(`${sql}=$${params.length}`); };

    if (nome    !== undefined) add('nome', nome);
    if (usuario !== undefined) add('usuario', usuario);
    if (email   !== undefined) add('email', email);
    if (role    !== undefined) add('role', role);
    if (role !== undefined && funcao === undefined) {
      funcao = roleToFuncao(role);
    }
    if (funcao  !== undefined) add('funcao', funcao);

    // Reset de senha se informado
    if (typeof senha === 'string') {
      if (senha.trim().length < 6) return res.status(400).json({ error: 'Senha muito curta.' });
      const hash = await bcrypt.hash(senha.trim(), 10);
      add('senha_hash', hash);
    }

    if (!sets.length) return res.status(400).json({ error: 'Nada para atualizar.' });

    params.push(id);
    const { rows } = await pool.query(
      `UPDATE usuarios SET ${sets.join(', ')}
        WHERE id=$${params.length}
      RETURNING id, nome, usuario, email, role, funcao`,
      params
    );

    if (!rows.length) return res.status(404).json({ error: 'UsuÃ¡rio nÃ£o encontrado.' });
    res.json(rows[0]);
  } catch (e: any) {
    if (String(e?.code) === '23505') {
      return res.status(409).json({ error: 'UsuÃ¡rio ou e-mail jÃ¡ existente.' });
    }
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// DELETE /usuarios/:id (gestor)
usuariosRouter.delete('/usuarios/:id', async (req, res) => {
  try {
    const auth = (req as any).user || {};
    if (auth.role !== 'gestor') {
      return res.status(403).json({ error: 'Somente gestor.' });
    }

    const id = String(req.params.id);
    const ts = Date.now(); // sufixo para evitar conflito de unique

    const upd = await pool.query(
      `UPDATE usuarios
          SET ativo   = false,
              email   = CASE WHEN email   IS NOT NULL THEN email   || '.inactive.' || $2 ELSE email   END,
              usuario = CASE WHEN usuario IS NOT NULL THEN usuario || '.inactive.' || $2 ELSE usuario END
        WHERE id = $1 AND ativo = true
        RETURNING id`,
      [id, ts]
    );

    if (!upd.rowCount) {
      return res.status(404).json({ error: 'UsuÃ¡rio nÃ£o encontrado ou jÃ¡ inativo.' });
    }
    res.json({ ok: true, id });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});



