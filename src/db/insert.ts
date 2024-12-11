import { validateDate } from '../utils/validate';
import { client } from './client';

// Variáveis de contagem para sucesso e falhas
let successCount = 0;
let failureCount = 0;
let totalInserted = 0; // Total de registros inseridos no banco

// Conjunto para armazenar os IDs já processados
const processedIds: Set<string> = new Set();
const duplicateIds: { [id: string]: number } = {}; // Para contar as duplicatas

export const insertBatchToDatabase = async (data: any[], tableName: string) => {
  const insertPromises: Promise<any>[] = [];
  const values: string[] = [];

  data.forEach((row) => {
    const id = row['id'];

    // Se o ID já foi processado, conta como duplicado
    if (processedIds.has(id)) {
      duplicateIds[id] = (duplicateIds[id] || 1) + 1;
      failureCount++; // Incrementa o contador de falhas (registro duplicado)
      return; // Ignora a inserção do registro duplicado
    }

    // Se o ID ainda não foi processado, adiciona ao conjunto de IDs processados
    processedIds.add(id);

    const valueString = Object.keys(row)
      .map((key) => {
        let value = row[key];

        // Se o valor for um número e for inválido (ex: "-"), substitui por NULL
        if (typeof value === 'string' && value.trim() === '-') {
          value = null; // Ou você pode definir um valor padrão se preferir
        }

        // Valida campos de data como 'created_at', 'updated_at', 'deleted_at'
        if (['created_at', 'updated_at', 'deleted_at'].includes(key)) {
          value = validateDate(value);
        }

        // Log de valor inválido ou não esperado
        if (value === null || value === undefined || value === '') {
          console.log(
            `Valor inválido ou vazio na coluna '${key}' com valor: '${value}'`,
          );
          return 'NULL';
        } else if (typeof value === 'string') {
          return `'${value.replace(/'/g, "''")}'`; // Escapa apóstrofos
        } else if (typeof value === 'boolean') {
          return value ? 'TRUE' : 'FALSE';
        } else if (typeof value === 'number') {
          return value.toString();
        } else {
          console.log(`Tipo inesperado na coluna '${key}', valor: '${value}'`);
          return `'${value}'`;
        }
      })
      .join(', ');

    values.push(`(${valueString})`);
    successCount++; // Incrementa o contador de sucesso para cada registro válido processado
  });

  // Se houver registros para inserir, prepara e executa a query SQL
  if (values.length > 0) {
    const sql = `INSERT INTO public.${tableName} (${Object.keys(data[0]).join(', ')}) VALUES ${values.join(', ')};`;
    insertPromises.push(
      client.query(sql).catch((err) => {
        console.error('Erro ao tentar inserir dados:', err);
        failureCount++; // Incrementa o contador de falhas
      }),
    );
  }

  // Espera a execução de todas as promessas de inserção
  await Promise.all(insertPromises);

  totalInserted += data.length; // Atualiza o total de registros inseridos
  console.log(`Lote de ${data.length} registros inserido com sucesso.`);
  console.log(`Total de registros inseridos até agora: ${totalInserted}`);
  console.log(`Total de registros processados: ${successCount}`);
  console.log(
    `Total de registros duplicados encontrados: ${Object.keys(duplicateIds).length}`,
  );
  console.log(`Total de falhas (duplicados e outros erros): ${failureCount}`);
};
