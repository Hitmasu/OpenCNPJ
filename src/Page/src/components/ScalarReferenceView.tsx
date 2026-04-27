import { ApiReferenceReact, type AnyApiReferenceConfiguration } from '@scalar/api-reference-react';
import '@scalar/api-reference-react/style.css';

interface ScalarReferenceViewProps {
  configuration: AnyApiReferenceConfiguration;
}

export function ScalarReferenceView({ configuration }: ScalarReferenceViewProps) {
  return <ApiReferenceReact configuration={configuration} />;
}
