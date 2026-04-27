import { FormEvent, useEffect, useMemo, useState } from 'react';
import { datasetDetails, datasetOrder } from '../data/datasets';
import type { DatasetKey } from '../types';
import { maskCNPJ, removeMask, validateCNPJ } from '../utils/cnpj';

const API_BASE_URL = 'https://api.opencnpj.org';
const DEFAULT_CNPJ = '00.000.000/0001-91';
const REQUEST_TIMEOUT_MS = 12_000;

interface InlineTryItProps {
  datasetKey?: DatasetKey;
  onCnpjChange?: (cnpj: string) => void;
}

interface TryItResult {
  body: string;
  elapsedMs?: number;
  ok: boolean;
  statusLabel: string;
}

function initialDataset(datasetKey?: DatasetKey) {
  return datasetKey ?? 'receita';
}

function formatResponseBody(text: string) {
  if (!text) {
    return '(sem corpo)';
  }

  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

function buildRequestUrl(cnpjInput: string, selectedDataset: DatasetKey) {
  const rawCnpj = removeMask(cnpjInput) || removeMask(DEFAULT_CNPJ);
  const params = new URLSearchParams();

  params.set('dataset', selectedDataset);

  const query = params.toString();
  return `${API_BASE_URL}/${rawCnpj}${query ? `?${query}` : ''}`;
}

export function InlineTryIt({ datasetKey, onCnpjChange }: InlineTryItProps) {
  const [cnpj, setCnpj] = useState(DEFAULT_CNPJ);
  const [selectedDataset, setSelectedDataset] = useState<DatasetKey>(() => initialDataset(datasetKey));
  const [isLoading, setIsLoading] = useState(false);
  const [result, setResult] = useState<TryItResult | null>(null);

  useEffect(() => {
    setSelectedDataset(initialDataset(datasetKey));
  }, [datasetKey]);

  useEffect(() => {
    onCnpjChange?.(removeMask(cnpj));
  }, [cnpj, onCnpjChange]);

  const requestUrl = useMemo(() => buildRequestUrl(cnpj, selectedDataset), [cnpj, selectedDataset]);

  async function executeRequest(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!validateCNPJ(cnpj)) {
      setResult({
        body: 'Informe um CNPJ válido com 14 caracteres. O CNPJ alfanumérico também é aceito.',
        ok: false,
        statusLabel: 'Validação',
      });
      return;
    }

    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    const startedAt = performance.now();

    try {
      setIsLoading(true);
      setResult(null);

      const response = await fetch(requestUrl, {
        headers: { Accept: 'application/json' },
        signal: controller.signal,
      });
      const body = await response.text();

      setResult({
        body: formatResponseBody(body),
        elapsedMs: Math.round(performance.now() - startedAt),
        ok: response.ok,
        statusLabel: `HTTP ${response.status}`,
      });
    } catch (error) {
      setResult({
        body: error instanceof Error && error.name === 'AbortError'
          ? 'Tempo limite excedido ao executar a chamada.'
          : 'Falha ao executar a chamada a partir do navegador.',
        ok: false,
        statusLabel: 'Erro',
      });
    } finally {
      window.clearTimeout(timeout);
      setIsLoading(false);
    }
  }

  return (
    <form className="inline-tryit" onSubmit={executeRequest}>
      <div className="inline-tryit-url" aria-label="URL da chamada">
        <code>GET {requestUrl}</code>
      </div>

      <div className="inline-tryit-controls">
        <label className="field">
          CNPJ
          <input
            type="text"
            inputMode="text"
            placeholder="12.ABC.345/01DE-35"
            value={cnpj}
            onChange={(event) => setCnpj(maskCNPJ(event.target.value))}
            aria-label="CNPJ para executar a API"
          />
        </label>
        <button className={`btn primary${isLoading ? ' loading' : ''}`} type="submit" disabled={isLoading}>
          Executar
        </button>
      </div>

      {datasetKey ? (
        <p className="hint">Dataset fixo: <code>{datasetDetails[datasetKey].filter}</code></p>
      ) : (
        <>
          <label className="field inline-tryit-dataset">
            Dataset
            <select
              value={selectedDataset}
              onChange={(event) => setSelectedDataset(event.target.value as DatasetKey)}
              aria-label="Dataset para executar a API"
            >
              {datasetOrder.map((key) => (
                <option key={key} value={key}>{datasetDetails[key].shortName}</option>
              ))}
            </select>
          </label>
          <p className="hint">Sem parâmetro, o retorno padrão documentado é Receita.</p>
        </>
      )}

      <details className="inline-tryit-details">
        <summary>Request</summary>
        <pre className="inline-tryit-code">{`GET ${requestUrl}
Accept: application/json`}</pre>
      </details>

      {result ? (
        <section className={`inline-tryit-response${result.ok ? ' ok' : ' error'}`} aria-live="polite">
          <div>
            <strong>{result.statusLabel}</strong>
            {typeof result.elapsedMs === 'number' ? <span>{result.elapsedMs} ms</span> : null}
          </div>
          <pre className="inline-tryit-code">{result.body}</pre>
        </section>
      ) : null}
    </form>
  );
}
