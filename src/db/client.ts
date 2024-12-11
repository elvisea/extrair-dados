import { Client } from 'pg';
import { config } from '../config';

// Configuração do cliente PostgreSQL
export const client = new Client({
  host: config.dbHost, // ou o endereço do seu servidor PostgreSQL
  port: config.dbPort,
  user: config.dbUser,
  password: config.dbPassword,
  database: config.dbName,
});
