declare namespace NodeJS {
  interface ProcessEnv {
    PGHOST?: string;
    PGPORT?: string;
    PGDATABASE?: string;
    PGUSER?: string;
    PGPASSWORD?: string;
    PORT?: string;
  }
}
