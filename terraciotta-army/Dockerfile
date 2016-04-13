FROM node:4.4.2-slim
MAINTAINER Canadian Tire Innovations

COPY . /app
WORKDIR /app
RUN npm install --production

ENTRYPOINT ["node", "index.js"]
