import type { NextFunction, Request, Response } from "express";
import { pool } from "../db";

type Role = "operador" | "manutentor" | "gestor" | "admin";

const VALID_ROLES: Role[] = ["operador", "manutentor", "gestor", "admin"];
const ROLE_SET = new Set<Role>(VALID_ROLES);
const AUTH_STRICT = String(process.env.AUTH_STRICT ?? "true").toLowerCase() !== "false";

function normalizeRole(value: string | undefined | null): Role {
  const candidate = String(value ?? "").trim().toLowerCase();
  return ROLE_SET.has(candidate as Role) ? (candidate as Role) : "operador";
}

export async function userFromHeader(req: Request, res: Response, next: NextFunction) {
  try {
    const hdrEmailRaw = req.header("x-user-email");
    const hdrRoleRaw = req.header("x-user-role");

    const email = hdrEmailRaw?.trim();
    const roleFromHeader = normalizeRole(hdrRoleRaw);

    if (!email) {
      req.user = undefined;
      return next();
    }

    const { rows } = await pool.query(
      `SELECT id, nome FROM public.usuarios WHERE lower(email) = lower($1) LIMIT 1`,
      [email]
    );

    if (!rows.length) {
      if (AUTH_STRICT) {
        return res.status(401).json({ error: "USUARIO_NAO_CADASTRADO" });
      }

      req.user = { id: undefined, email, name: null, role: roleFromHeader };
      return next();
    }

    const row = rows[0];

    req.user = {
      id: row.id,
      email,
      name: row.nome ?? null,
      role: roleFromHeader,
    };

    return next();
  } catch (error) {
    return next(error);
  }
}
