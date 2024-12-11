// src/utils/validate.ts

export const validateDate = (dateStr: string | null | undefined): string => {
  if (!dateStr || isNaN(Date.parse(dateStr))) {
    return `${new Date().toISOString()}`;
  }
  return `'${dateStr}'`;
};
