# syntax=docker/dockerfile:1

# 1) Instala dependências (dev) para compilar TypeScript
FROM node:20-alpine AS deps
WORKDIR /app
COPY package*.json ./
RUN npm ci

# 2) Build TS -> JS (gera dist/)
FROM node:20-alpine AS build
WORKDIR /app
# precisamos do package.json aqui para "npm run build"
COPY package*.json ./
COPY --from=deps /app/node_modules ./node_modules
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# 3) Runtime leve só com deps de produção + dist
FROM node:20-alpine AS prod
WORKDIR /app

ENV NODE_ENV=production \
    PORT=3000

# usuário não-root
RUN addgroup -S app && adduser -S app -G app

# deps de produção apenas
COPY package*.json ./
RUN npm ci --omit=dev

# app compilado
COPY --from=build /app/dist ./dist

# healthcheck (usa curl)
RUN apk add --no-cache curl
EXPOSE 3000
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -fsS http://localhost:3000/health || exit 1

USER app
CMD ["node", "dist/index.js"]
