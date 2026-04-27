import type { AnyApiReferenceConfiguration } from '@scalar/api-reference-react';
import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { datasetDetails } from '../data/datasets';
import type { DatasetKey } from '../types';
import { InlineTryIt } from './InlineTryIt';

const ScalarReferenceView = lazy(() =>
  import('./ScalarReferenceView').then((module) => ({ default: module.ScalarReferenceView })),
);

const OPENAPI_URL = '/openapi.json';
const DEFAULT_CNPJ = '00000000000191';
const RESPONSE_STATUS_CLASSES = [
  'scalar-response-status',
  'scalar-response-status--info',
  'scalar-response-status--success',
  'scalar-response-status--redirect',
  'scalar-response-status--client-error',
  'scalar-response-status--server-error',
];

type OpenApiDocument = Record<string, any>;

interface ScalarReferenceRailProps {
  title: string;
  description?: string;
  datasetKey?: DatasetKey;
}

function isOpenApiDocument(value: unknown): value is OpenApiDocument {
  return typeof value === 'object' && value !== null;
}

function getResponseStatusClass(statusCode: string) {
  const family = statusCode.charAt(0);

  if (family === '1') {
    return 'scalar-response-status--info';
  }

  if (family === '2') {
    return 'scalar-response-status--success';
  }

  if (family === '3') {
    return 'scalar-response-status--redirect';
  }

  if (family === '4') {
    return 'scalar-response-status--client-error';
  }

  if (family === '5') {
    return 'scalar-response-status--server-error';
  }

  return null;
}

function markScalarResponseStatuses(container: HTMLElement | null) {
  if (!container) {
    return;
  }

  container.querySelectorAll<HTMLElement>('[aria-label="Responses"] > .parameter-item').forEach((item) => {
    const statusName = item.querySelector<HTMLElement>('.parameter-item-name');
    const statusCode = statusName?.textContent?.match(/\b([1-5]\d{2})\b/)?.[1];
    const statusClass = statusCode ? getResponseStatusClass(statusCode) : null;

    item.classList.remove(...RESPONSE_STATUS_CLASSES);

    if (statusName && statusCode && statusClass) {
      item.classList.add('scalar-response-status', statusClass);
      statusName.dataset.responseStatus = statusCode;
    } else if (statusName) {
      delete statusName.dataset.responseStatus;
    }
  });
}

function cloneDocument(document: OpenApiDocument): OpenApiDocument {
  return JSON.parse(JSON.stringify(document)) as OpenApiDocument;
}

function getDatasetDefault(datasetKey?: DatasetKey) {
  return datasetKey ?? 'receita';
}

function buildEndpointUrl(datasetKey?: DatasetKey, cnpj = DEFAULT_CNPJ) {
  const datasetDefault = getDatasetDefault(datasetKey);

  return `https://api.opencnpj.org/${cnpj || DEFAULT_CNPJ}?dataset=${datasetDefault}`;
}

function buildCodeSamples(datasetKey?: DatasetKey, cnpj?: string) {
  const url = buildEndpointUrl(datasetKey, cnpj);

  return [
    {
      lang: 'Shell',
      label: 'cURL',
      source: `curl -s '${url}'`,
    },
    {
      lang: 'JavaScript',
      label: 'fetch',
      source: `const response = await fetch('${url}', {
  headers: { Accept: 'application/json' },
});

const data = await response.json();
console.log(data);`,
    },
    {
      lang: 'Python',
      label: 'requests',
      source: `import requests

response = requests.get('${url}', headers={'Accept': 'application/json'})
response.raise_for_status()
print(response.json())`,
    },
    {
      lang: 'C#',
      label: 'HttpClient',
      source: `using var http = new HttpClient();
using var request = new HttpRequestMessage(HttpMethod.Get, "${url}");
request.Headers.Accept.ParseAdd("application/json");

using var response = await http.SendAsync(request);
response.EnsureSuccessStatusCode();

var json = await response.Content.ReadAsStringAsync();
Console.WriteLine(json);`,
    },
  ];
}

function getOperationTitle(datasetKey?: DatasetKey) {
  if (!datasetKey) {
    return 'Consultar CNPJ';
  }

  return `Consultar ${datasetDetails[datasetKey].shortName}`;
}

function updateSimpleResponseExample(operation: OpenApiDocument, cnpj: string) {
  const exampleValue = operation.responses?.['200']?.content?.['application/json']?.examples?.simples?.value;

  if (isOpenApiDocument(exampleValue)) {
    exampleValue.cnpj = cnpj || DEFAULT_CNPJ;
  }
}

function buildRailDocument(document: OpenApiDocument, datasetKey?: DatasetKey, cnpj = DEFAULT_CNPJ) {
  const spec = cloneDocument(document);
  const cnpjOperation = spec.paths?.['/{cnpj}']?.get as OpenApiDocument | undefined;

  if (spec.paths?.['/{cnpj}']) {
    spec.paths = { '/{cnpj}': spec.paths['/{cnpj}'] };
  }

  spec.tags = [{ name: 'Consulta', description: 'Consulta pontual por CNPJ.' }];
  spec.info = {
    ...(isOpenApiDocument(spec.info) ? spec.info : {}),
    title: datasetKey ? `OpenCNPJ API - ${datasetDetails[datasetKey].shortName}` : 'OpenCNPJ API',
    description: 'Execute uma consulta por CNPJ usando o contrato público do OpenCNPJ.',
  };

  if (cnpjOperation) {
    cnpjOperation.summary = getOperationTitle(datasetKey);
    delete cnpjOperation.description;
    delete cnpjOperation.responses?.['429'];
    cnpjOperation['x-codeSamples'] = buildCodeSamples(datasetKey, cnpj);
    updateSimpleResponseExample(cnpjOperation, cnpj);

    if (Array.isArray(cnpjOperation.parameters)) {
      const cnpjParam = cnpjOperation.parameters.find((param: OpenApiDocument) => param.name === 'cnpj');
      const datasetParam = cnpjOperation.parameters.find((param: OpenApiDocument) => param.name === 'dataset');
      const datasetDefault = getDatasetDefault(datasetKey);

      if (isOpenApiDocument(cnpjParam)) {
        cnpjParam.example = cnpj || DEFAULT_CNPJ;
        delete cnpjParam.examples;
        cnpjParam.schema = {
          ...(isOpenApiDocument(cnpjParam.schema) ? cnpjParam.schema : {}),
          default: cnpj || DEFAULT_CNPJ,
        };
        delete cnpjParam.schema.examples;
      }

      if (isOpenApiDocument(datasetParam)) {
        datasetParam.example = datasetDefault;
        delete datasetParam.examples;
        datasetParam.schema = {
          ...(isOpenApiDocument(datasetParam.schema) ? datasetParam.schema : {}),
          default: datasetDefault,
          enum: ['receita', 'cno', 'rntrc'],
        };
      }
    }
  }

  return spec;
}

export function ScalarReferenceRail({ title, description, datasetKey }: ScalarReferenceRailProps) {
  const [sourceDocument, setSourceDocument] = useState<OpenApiDocument | null>(null);
  const [exampleCnpj, setExampleCnpj] = useState(DEFAULT_CNPJ);
  const [error, setError] = useState<string | null>(null);
  const referenceBodyRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const controller = new AbortController();

    async function loadDocument() {
      try {
        setError(null);
        const response = await fetch(OPENAPI_URL, { cache: 'no-store', signal: controller.signal });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const payload: unknown = await response.json();

        if (!isOpenApiDocument(payload)) {
          throw new Error('Documento de referência inválido.');
        }

        setSourceDocument(payload);
      } catch (loadError) {
        if (controller.signal.aborted) {
          return;
        }

        setError(loadError instanceof Error ? loadError.message : 'Falha ao carregar a referência da API.');
      }
    }

    loadDocument();

    return () => controller.abort();
  }, []);

  const handleCnpjChange = useCallback((cnpj: string) => {
    setExampleCnpj(cnpj || DEFAULT_CNPJ);
  }, []);

  const document = useMemo(
    () => (sourceDocument ? buildRailDocument(sourceDocument, datasetKey, exampleCnpj) : null),
    [datasetKey, exampleCnpj, sourceDocument],
  );

  const configuration = useMemo<AnyApiReferenceConfiguration | null>(() => {
    if (!document) {
      return null;
    }

    return {
      content: document,
      slug: datasetKey ? `opencnpj-${datasetKey}-tryit` : 'opencnpj-api-tryit',
      title: datasetKey ? `OpenCNPJ ${datasetDetails[datasetKey].shortName}` : 'OpenCNPJ API',
      agent: { disabled: true, hideAddApi: true },
      defaultHttpClient: { targetKey: 'shell', clientKey: 'curl' },
      defaultOpenAllTags: true,
      documentDownloadType: 'none',
      expandAllResponses: false,
      forceDarkModeState: 'dark',
      hideClientButton: true,
      hideDarkModeToggle: true,
      hideModels: true,
      hideSearch: true,
      hideTestRequestButton: true,
      layout: 'modern',
      mcp: { disabled: true },
      showDeveloperTools: 'never',
      showOperationId: false,
      showSidebar: false,
      telemetry: false,
      theme: 'none',
      withDefaultFonts: false,
    };
  }, [datasetKey, document]);

  useEffect(() => {
    const container = referenceBodyRef.current;

    if (!container || !configuration) {
      return undefined;
    }

    markScalarResponseStatuses(container);

    const observer = new MutationObserver(() => {
      markScalarResponseStatuses(container);
    });

    observer.observe(container, { childList: true, subtree: true });

    return () => observer.disconnect();
  }, [configuration]);

  return (
    <section className="scalar-reference-shell" aria-label={title}>
      <div className="scalar-reference-header">
        <div>
          <h3>{title}</h3>
          {description ? <p>{description}</p> : null}
        </div>
        <code>Scalar</code>
      </div>

      <InlineTryIt datasetKey={datasetKey} onCnpjChange={handleCnpjChange} />

      <div ref={referenceBodyRef} className="scalar-reference-body" aria-live="polite">
        {error ? (
          <div className="scalar-reference-state">
            <p>Não foi possível carregar a referência interativa.</p>
          </div>
        ) : configuration ? (
          <Suspense fallback={<div className="scalar-reference-state">Carregando Scalar…</div>}>
            <ScalarReferenceView key={`${datasetKey ?? 'todos'}-${exampleCnpj}`} configuration={configuration} />
          </Suspense>
        ) : (
          <div className="scalar-reference-state">Carregando referência da API…</div>
        )}
      </div>
    </section>
  );
}
