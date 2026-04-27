import { useEffect, useRef, useState } from 'react';
import { Layout } from './components/Layout';
import { AboutPage } from './pages/AboutPage';
import { AnalyticsPage } from './pages/AnalyticsPage';
import { ApiPage } from './pages/ApiPage';
import { DatasetPage } from './pages/DatasetPage';
import { FaqPage } from './pages/FaqPage';
import { LibrariesPage } from './pages/LibrariesPage';
import { LimitsPage } from './pages/LimitsPage';
import type { DatasetKey, PublishedInfo } from './types';
import { DEFAULT_ROUTE, normalizeRoute, toHash } from './utils/router';

const API_BASE_URL = 'https://api.opencnpj.org';

function resolveDatasetRoute(route: string): DatasetKey | null {
  const match = route.match(/^\/datasets\/(receita|cno|rntrc)$/);
  return match ? (match[1] as DatasetKey) : null;
}

function isDocumentationRoute(route: string) {
  return [
    '/sobre',
    '/api',
    '/limites',
    '/consultas-analiticas',
    '/bibliotecas',
    '/faq',
  ].includes(route) || resolveDatasetRoute(route) !== null;
}

function resolveDocumentationRoute(route: string) {
  if (route === '/datasets') {
    return '/datasets/receita';
  }

  return isDocumentationRoute(route) ? route : null;
}

function getInitialRoute() {
  const route = normalizeRoute(window.location.hash);
  return resolveDocumentationRoute(route) ?? DEFAULT_ROUTE;
}

export function App() {
  const [activeRoute, setActiveRoute] = useState(getInitialRoute);
  const activeRouteRef = useRef(activeRoute);
  const [info, setInfo] = useState<PublishedInfo | null>(null);
  const [isInfoLoading, setIsInfoLoading] = useState(true);

  useEffect(() => {
    if (!window.location.hash || normalizeRoute(window.location.hash) !== activeRouteRef.current) {
      window.history.replaceState(null, '', toHash(activeRouteRef.current));
    }

    function handleHashChange() {
      const nextRoute = resolveDocumentationRoute(normalizeRoute(window.location.hash));

      if (!nextRoute) {
        window.history.replaceState(null, '', toHash(activeRouteRef.current));
        return;
      }

      activeRouteRef.current = nextRoute;
      setActiveRoute(nextRoute);

      const canonicalHash = toHash(nextRoute);
      if (window.location.hash !== canonicalHash) {
        window.history.replaceState(null, '', canonicalHash);
      }
    }

    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);

  useEffect(() => {
    const controller = new AbortController();

    async function loadInfo() {
      try {
        const response = await fetch(`${API_BASE_URL}/info`, {
          headers: { Accept: 'application/json' },
          signal: controller.signal,
        });
        const payload = (await response.json()) as PublishedInfo;
        setInfo(payload);
      } catch {
        if (!controller.signal.aborted) setInfo(null);
      } finally {
        if (!controller.signal.aborted) setIsInfoLoading(false);
      }
    }

    loadInfo();
    return () => controller.abort();
  }, []);

  const datasetRoute = resolveDatasetRoute(activeRoute);
  const page = (() => {
    if (datasetRoute) {
      return <DatasetPage datasetKey={datasetRoute} info={info} isLoading={isInfoLoading} />;
    }

    switch (activeRoute) {
      case '/sobre':
        return <AboutPage />;
      case '/api':
        return <ApiPage />;
      case '/limites':
        return <LimitsPage />;
      case '/consultas-analiticas':
        return <AnalyticsPage />;
      case '/bibliotecas':
        return <LibrariesPage />;
      case '/faq':
        return <FaqPage />;
      default:
        return <AboutPage />;
    }
  })();

  return <Layout activeRoute={activeRoute}>{page}</Layout>;
}
