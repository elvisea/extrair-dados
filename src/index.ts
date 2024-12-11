import * as dotenv from 'dotenv';

import { client } from './db/client';
import { extractDataFromFile } from './utils/parseCsv';

dotenv.config();

if (!process.env.INPUT_FILE_PATH || !process.env.TABLE_NAME || !process.env.URL) {
  console.error(
    'Erro: Faltam variáveis de ambiente necessárias (INPUT_FILE_PATH ou TABLE_NAME).',
  );
  process.exit(1);
}

const processCsvFile = async (inputPath: string, tableName: string, url: string) => {
  const startTime = Date.now();

  let successCount = 0;
  let failureCount = 0;

  try {
    console.log('Iniciando o processamento do arquivo CSV...');
    await client.connect();
    console.log('Conectado ao banco de dados com sucesso!');

    await extractDataFromFile(inputPath, tableName, url, {
      onSuccess: (count: number) => {
        successCount += count;
      },
      onFailure: (count: number) => {
        failureCount += count;
      },
    });

    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    console.log(`Processamento concluído em ${duration} segundos.`);

    console.log(`Total de registros inseridos com sucesso: ${successCount}`);
    console.log(`Total de falhas: ${failureCount}`);
  } catch (error) {
    console.error('Erro ao processar o arquivo CSV:', error);
  } finally {
    await client.end();
    console.log('Conexão com o banco de dados encerrada.');
  }
};

// Usando as variáveis de ambiente para inputFilePath e tableName
const tableName = process.env.TABLE_NAME || 'default_table';
const url = process.env.URL || 'http://localhost:3000/accessories';
const inputFilePath = process.env.INPUT_FILE_PATH || './input/default.csv';

processCsvFile(inputFilePath, tableName, url);
