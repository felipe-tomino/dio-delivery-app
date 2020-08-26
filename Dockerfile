FROM node:12

RUN mkdir -p /home/node/app/node_modules && chown -R node:node /home/node/app

WORKDIR /home/node/app

COPY package*.json ./

RUN apt-get update && apt-get install libsasl2-dev libsasl2-modules

USER node

RUN yarn

COPY --chown=node:node . .

# EXPOSE 8080
# CMD [ "yarn", "prod" ]
