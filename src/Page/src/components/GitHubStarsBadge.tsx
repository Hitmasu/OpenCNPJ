import { useEffect, useState } from 'react';
import { GitHubIcon, StarIcon } from './Icons';

const REPOSITORY_API_URL = 'https://api.github.com/repos/Hitmasu/OpenCNPJ';
const REPOSITORY_URL = 'https://github.com/Hitmasu/OpenCNPJ';

interface GitHubRepositoryPayload {
  stargazers_count?: number;
}

function formatStars(count: number) {
  return new Intl.NumberFormat('pt-BR').format(count);
}

export function GitHubStarsBadge() {
  const [stars, setStars] = useState<number | null>(null);

  useEffect(() => {
    const controller = new AbortController();

    async function loadStars() {
      try {
        const response = await fetch(REPOSITORY_API_URL, {
          headers: { Accept: 'application/vnd.github+json' },
          signal: controller.signal,
        });

        if (!response.ok) {
          return;
        }

        const payload = (await response.json()) as GitHubRepositoryPayload;
        if (typeof payload.stargazers_count === 'number') {
          setStars(payload.stargazers_count);
        }
      } catch {
        if (!controller.signal.aborted) {
          setStars(null);
        }
      }
    }

    loadStars();
    return () => controller.abort();
  }, []);

  const label = stars === null ? '--' : formatStars(stars);

  return (
    <a className="github-badge" href={REPOSITORY_URL} target="_blank" rel="noopener" aria-label={`Abrir repositório OpenCNPJ no GitHub, ${label} favoritos`}>
      <GitHubIcon />
      <span>Ver no GitHub</span>
      <span className="github-stars" aria-hidden="true">
        <StarIcon />
        {label}
      </span>
    </a>
  );
}
