FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json nest-cli.json ./
COPY apps/notification-service ./apps/notification-service
COPY libs ./libs

RUN npx nest build notification-service

CMD ["node", "dist/apps/notification-service/src/main.js"]
