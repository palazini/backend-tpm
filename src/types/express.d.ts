import 'express';

declare module 'express-serve-static-core' {
  interface Request {
    user?: {
      id?: string;
      email?: string;
      name?: string | null;
      role?: 'operador' | 'manutentor' | 'gestor' | 'admin';
    };
  }
}

export {};
