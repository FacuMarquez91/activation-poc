FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json nest-cli.json ./
COPY apps/provisioning-service ./apps/provisioning-service
COPY libs ./libs

RUN npx nest build provisioning-service

CMD ["node", "dist/apps/provisioning-service/src/main.js"]
