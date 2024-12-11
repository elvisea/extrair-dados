export const normalizeKeys = (data: any): any => {
  const normalizedData: any = {};

  // Função para converter snake_case para camelCase
  const toCamelCase = (str: string): string => {
    // Caso específico para "AAIA_BrandID" que deve virar "aaiaBrandID"
    if (str === 'AAIA_BrandID') {
      return 'aaiaBrandID';
    }

    // Converte _ + letra para maiúscula e a primeira letra para minúscula
    return str
      .replace(/_./g, (match) => match.charAt(1).toUpperCase()) // Converte _ + letra para maiúscula
      .replace(/^./, (match) => match.toLowerCase()); // Converte a primeira letra para minúscula
  };

  // Função para corrigir camelCase para casos específicos
  const fixCamelCase = (str: string): string => {
    // Tratar casos específicos
    if (str === 'CID') {
      return 'cid'; // Caso específico para "CID"
    }

    if (str === 'CC') {
      return 'cc'; // Caso específico para "CC"
    }

    // Para outras chaves, a primeira letra será minúscula
    return str.charAt(0).toLowerCase() + str.slice(1);
  };

  // Função principal para processar as chaves do objeto
  for (const key in data) {
    if (data.hasOwnProperty(key)) {
      // Se a chave contém "_", converte para camelCase
      const normalizedKey = key.includes('_') ? toCamelCase(key) : fixCamelCase(key);
      normalizedData[normalizedKey] = data[key];
    }
  }

  return normalizedData;
};
