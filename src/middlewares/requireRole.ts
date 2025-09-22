import type { RequestHandler } from "express";

type Role = "operador" | "manutentor" | "gestor" | "admin";

function hasRequiredRole(userRole: Role | undefined, allowed: Role[]): boolean {
  if (!userRole) return false;
  if (userRole === "admin") return true;
  if (allowed.length === 0) return true;
  return allowed.includes(userRole);
}

export function requireRole(allowed: Role[]): RequestHandler {
  return (req, res, next) => {
    const user = req.user;

    if (!user?.id) {
      return res.status(401).json({ error: "USUARIO_NAO_CADASTRADO" });
    }

    if (!hasRequiredRole(user.role as Role | undefined, allowed)) {
      return res.status(403).json({ error: "PERMISSAO_NEGADA" });
    }

    next();
  };
}
