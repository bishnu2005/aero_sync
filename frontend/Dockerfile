FROM node:22-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
# Leave empty for the bundled Nginx proxy (same-origin /api and /ws).
ARG VITE_API_BASE_URL=
ARG VITE_WS_URL=
ARG VITE_MAP_TILE_DARK=
ARG VITE_MAP_TILE_LIGHT=
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL VITE_WS_URL=$VITE_WS_URL \
    VITE_MAP_TILE_DARK=$VITE_MAP_TILE_DARK VITE_MAP_TILE_LIGHT=$VITE_MAP_TILE_LIGHT
RUN npm run build

FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
