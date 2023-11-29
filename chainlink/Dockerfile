FROM node:21-bullseye
RUN ["git", "clone", "https://github.com/smartcontractkit/quickstarts-historical-prices-api.git"]
WORKDIR "quickstarts-historical-prices-api"
RUN ["yarn", "install"]
ENV NODE_ENV production
RUN ["yarn", "build"]

EXPOSE 3000
ENV PORT 3000
ENV HOSTNAME 0.0.0.0
CMD ["yarn", "start"]