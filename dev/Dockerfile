FROM node:20.12.1

WORKDIR /app
COPY package.json .
COPY package-lock.json .
COPY bin/ .
RUN npm ci --production

EXPOSE 8080
CMD ["node", "main_bin"]
