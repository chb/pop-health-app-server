FROM node:13

WORKDIR /app
COPY package.json /tmp/package.json
COPY package-lock.json /tmp/package-lock.json
RUN cd /tmp && npm install --production
RUN mv /tmp/node_modules /app/node_modules
COPY . .
EXPOSE 80 
CMD ["node", "./index.js"]
