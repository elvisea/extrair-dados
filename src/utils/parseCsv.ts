import * as fs from 'fs';
import axios from 'axios';  // Importando axios para fazer a requisição HTTP
import csvParser from 'csv-parser';
import { normalizeKeys } from './normalize';

// Função para limpar e formatar o campo description (ou outros campos como mid_code)
const formatDescription = (description: string): string => {
  let formattedDescription = description.replace(/[\s\t\r\n]+/g, ' ').trim();
  return formattedDescription;
};

// Função para corrigir campos JSON
const fixJsonField = (jsonString: string): string | null => {
  try {
    const cleanedJsonString = jsonString.replace(/[\s\t\r\n]+/g, ' ').trim();
    const parsedJson = JSON.parse(cleanedJsonString);
    return JSON.stringify(parsedJson);
  } catch (error) {
    console.error(`Erro ao corrigir o campo JSON: ${error}`);
    return null;
  }
};

// Função para converter valores vazios para NULL
// Função para converter valores vazios ou "-" para NULL ou 0
const convertEmptyStringsToNull = (data: any) => {
  Object.keys(data).forEach((key) => {
    if (data[key] === "") {
      data[key] = null;  // Converte os campos vazios para null
    }
    // Verifica se o valor é "-" e converte para null ou 0 (depende da lógica)
    if (data[key] === "-") {
      data[key] = null;  // Se preferir 0 ao invés de null, altere para data[key] = 0;
    }

    // Converte 'false' ou 'FALSE' para booleano false
    if (data[key] === "false" || data[key] === "FALSE") {
      data[key] = false;
    }

    // Converte 'true' ou 'TRUE' para booleano true
    if (data[key] === "true" || data[key] === "TRUE") {
      data[key] = true;
    }
  });
  return data;
};

export const extractDataFromFile = async (
  path: string,
  tableName: string,
  url: string,
  callbacks: {
    onSuccess: (count: number) => void;
    onFailure: (count: number) => void;
  },
): Promise<void> => {
  let lineCount = 0;
  let batchData: any[] = [];
  let successCount = 0;
  let failureCount = 0;

  // Array para armazenar as promessas de inserção
  const insertPromises: Promise<any>[] = [];

  return new Promise<void>((resolve, reject) => {
    fs.createReadStream(path)
      .pipe(csvParser())
      .on('data', async (data) => {
        lineCount++;
        console.log(`Linha lida. Total de linhas processadas: ${lineCount}`);

        // Processar o campo 'description'
        if (data['description']) {
          data['description'] = formatDescription(data['description']);
        }

        // Processar o campo 'mid_code' se necessário
        if (data['mid_code']) {
          data['mid_code'] = formatDescription(data['mid_code']);
        }

        // Se o campo 'metadata' existir e for um JSON, corrige o formato
        if (data['metadata']) {
          const metadata = data['metadata'];
          if (
            typeof metadata === 'string' &&
            metadata.trim().startsWith('{') &&
            metadata.trim().endsWith('}')
          ) {
            const correctedJson = fixJsonField(metadata);
            if (correctedJson !== null) {
              data['metadata'] = correctedJson;
            } else {
              console.log(`Campo 'metadata' não pôde ser corrigido.`);
            }
          }
        }

        // Converte campos vazios para NULL
        const normalizedData = normalizeKeys(data);
        const cleanedData = convertEmptyStringsToNull(normalizedData);

        batchData.push(cleanedData);

        // Se o batch atingir o limite, inicia o processo de inserção paralela
        if (batchData.length >= 500) {
          const currentBatch = [...batchData]; // Cria uma cópia do lote para inserção
          batchData = []; // Reseta o lote atual

          // Envia o lote para a API
          insertPromises.push(
            axios
              .post(url, currentBatch)
              .then(() => {
                successCount += currentBatch.length;
              })
              .catch((error) => {
                console.error(`Erro ao enviar dados para a API: ${error.message}`);
                failureCount += currentBatch.length;
              }),
          );
        }
      })
      .on('end', async () => {
        console.log(
          `Leitura do arquivo CSV concluída. Total de linhas processadas: ${lineCount}`,
        );

        if (batchData.length > 0) {
          // Envia o último lote se houver dados restantes
          const currentBatch = [...batchData];
          insertPromises.push(
            axios
              .post(url, currentBatch)
              .then(() => {
                successCount += currentBatch.length;
              })
              .catch((error) => {
                console.error(`Erro ao enviar dados para a API: ${error.message}`);
                failureCount += currentBatch.length;
              }),
          );
        }

        // Aguarda a conclusão de todas as promessas de inserção
        await Promise.all(insertPromises);

        callbacks.onSuccess(successCount);
        callbacks.onFailure(failureCount);

        resolve();
      })
      .on('error', async (err) => {
        console.error('Erro ao processar o arquivo CSV:', err);
        reject(err);
      });
  });
};
