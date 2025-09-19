import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';

import { userFromHeader } from './middlewares/userFromHeader';
import { eventsRouter } from './routes/events';
import { healthRouter } from './routes/health';
import { maquinasRouter } from './routes/maquinas';
import { chamadosRouter } from './routes/chamados';
import { agendamentosRouter } from './routes/agendamentos';
import { pecasRouter } from './routes/pecas';
import { causasRouter } from './routes/causas';
import { usuariosRouter } from './routes/usuarios';
import { authRouter } from './routes/auth';
import { checklistsRouter } from './routes/checklists';
import { analyticsRouter } from './routes/analytics';

dotenv.config();

const app = express();

app.use(cors());
app.use(express.json());
app.use(userFromHeader);

app.use(eventsRouter);
app.use(healthRouter);
app.use(maquinasRouter);
app.use(chamadosRouter);
app.use(agendamentosRouter);
app.use(pecasRouter);
app.use(causasRouter);
app.use(usuariosRouter);
app.use(authRouter);
app.use(checklistsRouter);
app.use(analyticsRouter);

export { app };
