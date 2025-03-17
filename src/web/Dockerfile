FROM node:16-alpine as build

# Set working directory
WORKDIR /app

# Copy package files for dependency installation
# This is separated from the source code copy to leverage Docker layer caching
COPY package*.json ./

# Install dependencies using clean install for reproducible builds
RUN npm ci

# Copy application source code
COPY . .

# Build the application
RUN npm run build

# Production stage: serve the static files with Nginx
FROM nginx:alpine

# Copy the built application from build stage to Nginx html directory
COPY --from=build /app/dist /usr/share/nginx/html

# Copy Nginx configuration
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Expose port 80 for HTTP traffic
EXPOSE 80

# Start Nginx server in foreground mode
CMD ["nginx", "-g", "daemon off;"]