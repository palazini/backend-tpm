import { Router } from 'express';
import bcrypt from 'bcryptjs';
import { pool } from '../db';

export const authRouter = Router();

authRouter.post('/auth/login', async (req, res) => {
  try {
    const raw   = String(req.body?.identifier || '').trim().toLowerCase();
    const senha = String(req.body?.senha || '');

    if (!raw)   return res.status(400).json({ error: 'Informe UsuÃ¡rio ou e-mail.' });
    if (senha.length < 6) return res.status(400).json({ error: 'Senha muito curta.' });

    // aceita "usuario" ou "email"
    const usuario = raw.includes('@') ? raw.split('@')[0] : raw;

    const { rows } = await pool.query(
      `SELECT id, nome, email, role,
              COALESCE(funcao,
                CASE
                  WHEN LOWER(role)='gestor'     THEN 'Gestor'
                  WHEN LOWER(role)='manutentor' THEN 'TÃ©cnico EletromecÃ¢nico'
                  ELSE 'Operador de CNC'
                END) AS funcao,
              COALESCE(usuario, split_part(LOWER(email),'@',1)) AS usuario,
              senha_hash
         FROM usuarios
        WHERE LOWER(email) = LOWER($1)
           OR LOWER(usuario) = LOWER($2)
        LIMIT 1`,
      [raw, usuario]
    );

    if (!rows.length) return res.status(401).json({ error: 'Credenciais invÃ¡lidas.' });

    const u = rows[0];

    // *** EXIGIR senha SEMPRE ***
    if (!u.senha_hash) {
      // antes permitia login sem senha; agora bloqueia
      return res.status(401).json({ error: 'Senha nÃ£o definida. Defina a senha primeiro.' });
    }
    const ok = await bcrypt.compare(senha, u.senha_hash);
    if (!ok) return res.status(401).json({ error: 'Credenciais invÃ¡lidas.' });

    return res.json({
      id: u.id, nome: u.nome, email: u.email, role: u.role, funcao: u.funcao, usuario: u.usuario
    });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

authRouter.post('/auth/change-password', async (req, res) => {
  try {
    const bodyEmail   = String(req.body?.email || '').trim().toLowerCase();
    const headerEmail = String(req.headers['x-user-email'] || '').trim().toLowerCase();
    const email       = bodyEmail || headerEmail;
    const senhaAtual  = String(req.body?.senhaAtual || '');
    const novaSenha   = String(req.body?.novaSenha || '');

    if (!email)      return res.status(400).json({ error: 'Informe o e-mail.' });
    if (novaSenha.length < 6) return res.status(400).json({ error: 'Nova senha muito curta.' });

    const { rows } = await pool.query(
      `SELECT id, senha_hash FROM usuarios WHERE LOWER(email)=LOWER($1) LIMIT 1`,
      [email]
    );
    if (!rows.length) return res.status(404).json({ error: 'UsuÃ¡rio nÃ£o encontrado.' });

    const u = rows[0];

    // Se jÃ¡ existe senha, exige confirmaÃ§Ã£o da atual
    if (u.senha_hash) {
      const ok = await bcrypt.compare(senhaAtual, u.senha_hash);
      if (!ok) return res.status(400).json({ error: 'Senha atual invÃ¡lida.' });
    }
    // Caso ainda nÃ£o exista senha (migraÃ§Ã£o), permitimos definir sem exigir a atual

    const hash = await bcrypt.hash(novaSenha, 10);
    await pool.query(`UPDATE usuarios SET senha_hash=$2 WHERE id=$1`, [u.id, hash]);

    res.json({ ok: true });
  } catch (e:any) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});


