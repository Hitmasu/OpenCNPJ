const BASE_LENGTH = 12;
const REGEX_BASE_CNPJ = /^[A-Z\d]{12}$/;
const REGEX_FULL_CNPJ = /^[A-Z\d]{12}\d{2}$/;
const REGEX_MASK_CHARACTERS = /[./-]/g;
const REGEX_INVALID_CHARACTERS = /[^A-Z\d./-]/i;
const ASCII_ZERO = '0'.charCodeAt(0);
const CHECK_DIGIT_WEIGHT = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2];
const ZEROED_CNPJ = '00000000000000';

export function removeMask(cnpj: string) {
  return (cnpj || '').replace(REGEX_MASK_CHARACTERS, '').toUpperCase();
}

export function maskCNPJ(value: string) {
  const raw = removeMask(value).slice(0, 14);
  let output = '';

  for (let index = 0; index < raw.length; index += 1) {
    output += raw[index];
    if (index === 1 && raw.length > 2) output += '.';
    if (index === 4 && raw.length > 5) output += '.';
    if (index === 7 && raw.length > 8) output += '/';
    if (index === 11 && raw.length > 12) output += '-';
  }

  return output;
}

function isRepeatedSequence(value: string) {
  return /^([A-Z\d])\1{13}$/i.test(value);
}

function calculateCheckDigits(baseCNPJ: string) {
  if (REGEX_INVALID_CHARACTERS.test(baseCNPJ)) {
    throw new Error('CNPJ contains invalid characters');
  }

  const raw = removeMask(baseCNPJ);
  if (!REGEX_BASE_CNPJ.test(raw) || raw === ZEROED_CNPJ.slice(0, BASE_LENGTH)) {
    throw new Error('Invalid base CNPJ for check digits calculation');
  }

  const digits = Array.from(raw).map((char) => char.charCodeAt(0) - ASCII_ZERO);
  const sum1 = digits.reduce(
    (acc, digit, index) => acc + digit * CHECK_DIGIT_WEIGHT[index + 1],
    0,
  );
  const dv1 = sum1 % 11 < 2 ? 0 : 11 - (sum1 % 11);
  const sum2 =
    digits.reduce((acc, digit, index) => acc + digit * CHECK_DIGIT_WEIGHT[index], 0) +
    dv1 * CHECK_DIGIT_WEIGHT[BASE_LENGTH];
  const dv2 = sum2 % 11 < 2 ? 0 : 11 - (sum2 % 11);

  return `${dv1}${dv2}`;
}

export function validateCNPJ(cnpj: string) {
  if (REGEX_INVALID_CHARACTERS.test(cnpj)) return false;

  const raw = removeMask(cnpj);
  if (!REGEX_FULL_CNPJ.test(raw) || raw === ZEROED_CNPJ) return false;
  if (isRepeatedSequence(raw)) return false;

  try {
    const base = raw.slice(0, BASE_LENGTH);
    const providedCheckDigits = raw.slice(BASE_LENGTH);
    return providedCheckDigits === calculateCheckDigits(base);
  } catch {
    return false;
  }
}
