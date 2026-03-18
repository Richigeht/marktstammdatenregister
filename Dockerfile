FROM nginx:1.27-alpine

COPY docs /usr/share/nginx/html

EXPOSE 80

HEALTHCHECK CMD wget -q -O /dev/null http://127.0.0.1/ || exit 1
