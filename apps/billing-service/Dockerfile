FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json nest-cli.json ./
COPY apps/billing-service ./apps/billing-service
COPY libs ./libs

RUN npx nest build billing-service

CMD ["node", "dist/apps/billing-service/src/main.js"]
