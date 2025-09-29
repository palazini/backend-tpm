import { Router } from 'express';
import { setupSSEClient } from '../utils/sse';

export const eventsRouter = Router();

eventsRouter.get('/events', (req, res) => {
  const cleanup = setupSSEClient(res);

  req.on('close', () => {
    cleanup();
  });
});
