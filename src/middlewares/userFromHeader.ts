import type { RequestHandler } from 'express';

declare global {
  namespace Express {
    interface UserContext {
      role: string;
      email: string | null;
    }

    interface Request {
      user?: UserContext;
    }
  }
}

export const userFromHeader: RequestHandler = (req, _res, next) => {
  req.user = {
    role: String(req.header('x-user-role') || 'operador'),
    email: req.header('x-user-email') ? String(req.header('x-user-email')) : null,
  };

  next();
};
