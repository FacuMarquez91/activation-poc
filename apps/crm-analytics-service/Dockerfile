FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json nest-cli.json ./
COPY apps/crm-analytics-service ./apps/crm-analytics-service
COPY libs ./libs

RUN npx nest build crm-analytics-service

CMD ["node", "dist/apps/crm-analytics-service/src/main.js"]
