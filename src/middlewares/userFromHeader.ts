import type { NextFunction, Request, Response } from "express";
import { pool } from "../db";

type Role = "operador" | "manutentor" | "gestor" | "admin";

const VALID_ROLES: Role[] = ["operador", "manutentor", "gestor", "admin"];
const ROLE_SET = new Set<Role>(VALID_ROLES);

// true (padrão) => exige usuário no DB; false => permite fallback (DEV)
const AUTH_STRICT = String(process.env.AUTH_STRICT ?? "true").toLowerCase() !== "false";

function normalizeRole(value: string | undefined | null): Role {
  const candidate = String(value ?? "").trim().toLowerCase();
  return ROLE_SET.has(candidate as Role) ? (candidate as Role) : "operador";
}

export async function userFromHeader(req: Request, res: Response, next: NextFunction) {
  try {
    const email = String(req.header("x-user-email") || "").trim();

    // se já foi autenticado por JWT e tem req.user, não mexe
    if ((req as any).user?.id) return next();

    if (!email) {
      (req as any).user = undefined;
      return next();
    }

    // 🔎 AGORA buscamos também o role no DB
    const { rows } = await pool.query(
      `SELECT id, nome, email, role
         FROM public.usuarios
        WHERE lower(email) = lower($1)
        LIMIT 1`,
      [email]
    );

    if (!rows.length) {
      if (AUTH_STRICT) {
        return res.status(401).json({ error: "USUARIO_NAO_CADASTRADO" });
      }
      // modo DEV (não-estrito): cria um usuário "temporário" com operador
      (req as any).user = { id: undefined, email, name: null, role: "operador" };
      return next();
    }

    const row = rows[0];

    // ✅ SEMPRE usar o role vindo do DB (ignorar x-user-role)
    (req as any).user = {
      id: row.id,
      email: row.email ?? email,
      name: row.nome ?? null,
      role: normalizeRole(row.role),
    };

    return next();
  } catch (error) {
    console.error("userFromHeader", error);
    return next(error);
  }
}
