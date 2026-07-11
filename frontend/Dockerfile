FROM node:22-alpine AS dependencies

WORKDIR /workspace/frontend

COPY frontend/package*.json ./
RUN npm ci

FROM node:22-alpine AS builder

WORKDIR /workspace/frontend

ARG NEXT_PUBLIC_API_BASE_URL=/api
ENV NEXT_PUBLIC_API_BASE_URL=${NEXT_PUBLIC_API_BASE_URL}

COPY --from=dependencies /workspace/frontend/node_modules ./node_modules
COPY frontend ./
RUN npm run build

FROM node:22-alpine AS runner

WORKDIR /workspace/frontend

ENV NODE_ENV=production
ENV TZ=Asia/Tokyo
ENV HOSTNAME=0.0.0.0
ENV PORT=3000

COPY --from=builder --chown=node:node /workspace/frontend/.next/standalone ./
COPY --from=builder --chown=node:node /workspace/frontend/.next/static ./.next/static

USER node

EXPOSE 3000

CMD ["node", "server.js"]
