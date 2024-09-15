FROM node:20
# RUN npm install -g dockerize
WORKDIR usr/src/app
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 3000
CMD ["node", "index.js"]
# CMD ["dockerize", "-wait", "tcp://mysql-db:3306", "-timeout", "30s", "node", "--harmony", "index.js"]