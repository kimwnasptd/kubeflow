#FROM node:12.20.1
FROM browserless/chrome:1.44-chrome-stable

WORKDIR /lib

COPY . /lib
RUN npm run test-ci
