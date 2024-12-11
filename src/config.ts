import * as dotenv from 'dotenv';

// Carrega as variáveis de ambiente do arquivo .env
dotenv.config();

if (
  !process.env.DB_HOST ||
  !process.env.DB_PORT ||
  !process.env.DB_USERNAME ||
  !process.env.DB_PASSWORD ||
  !process.env.DB_DATABASE
) {
  console.error(
    'Faltam variáveis de ambiente necessárias (DB_HOST ou DB_PORT ou DB_USERNAME ou DB_PASSWORD ou DB_DATABASE).',
  );
  process.exit(1);
}

export const config = {
  dbHost: process.env.DB_HOST,
  dbPort: Number(process.env.DB_PORT),
  dbUser: process.env.DB_USERNAME,
  dbPassword: process.env.DB_PASSWORD,
  dbName: process.env.DB_DATABASE,
};
