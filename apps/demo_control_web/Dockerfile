FROM node:22.17.1-alpine AS deps
WORKDIR /app
COPY apps/demo_control_web/package*.json ./
RUN npm install
FROM node:22.17.1-alpine AS build
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY apps/demo_control_web ./
ARG NEXT_PUBLIC_DEMO_API_URL=http://localhost:8082
ENV NEXT_PUBLIC_DEMO_API_URL=$NEXT_PUBLIC_DEMO_API_URL NEXT_TELEMETRY_DISABLED=1
RUN npm run build
FROM node:22.17.1-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production NEXT_TELEMETRY_DISABLED=1
RUN addgroup -S commerce && adduser -S commerce -G commerce
COPY --from=build --chown=commerce:commerce /app/.next/standalone ./
COPY --from=build --chown=commerce:commerce /app/.next/static ./.next/static
COPY --from=build --chown=commerce:commerce /app/public ./public
USER commerce
EXPOSE 3000
CMD ["node", "server.js"]
