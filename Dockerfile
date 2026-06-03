FROM node:20-bookworm

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

EXPOSE 8787

CMD ["node", "scripts/docker-start.mjs"]
