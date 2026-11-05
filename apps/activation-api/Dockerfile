FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json nest-cli.json ./
COPY apps/activation-api ./apps/activation-api
COPY libs ./libs

RUN npx nest build activation-api

EXPOSE 3000

CMD ["node", "dist/apps/activation-api/src/main.js"]
