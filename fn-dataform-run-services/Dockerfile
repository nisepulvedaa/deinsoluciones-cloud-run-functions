# Imagen base oficial de Node.js
FROM node:20

# Crear directorio de trabajo
WORKDIR /usr/src/app

# Copiar dependencias e instalarlas
COPY package*.json ./
RUN npm install

# Copiar el resto del código
COPY . .

# Exponer puerto
EXPOSE 8080

# Comando de inicio
CMD ["node", "index.js"]
