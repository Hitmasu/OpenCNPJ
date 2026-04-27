import type { MouseEvent, ReactNode } from 'react';
import { navigationItems } from '../data/navigation';
import { GitHubStarsBadge } from './GitHubStarsBadge';
import { HeartIcon, PixIcon } from './Icons';

interface LayoutProps {
  children: ReactNode;
  activeRoute: string;
}

function isActiveLink(href: string | undefined, activeRoute: string) {
  if (!href) {
    return false;
  }

  const route = href.replace(/^#/, '');
  return activeRoute === route || activeRoute.startsWith(`${route}/`);
}

function isActiveGroup(item: { href?: string; children?: { href?: string }[] }, activeRoute: string) {
  return isActiveLink(item.href, activeRoute) || Boolean(item.children?.some((child) => isActiveLink(child.href, activeRoute)));
}

export function Layout({ children, activeRoute }: LayoutProps) {
  function handleSkipLink(event: MouseEvent<HTMLAnchorElement>) {
    event.preventDefault();
    const content = document.getElementById('conteudo');
    content?.focus({ preventScroll: true });
    content?.scrollIntoView({ block: 'start' });
  }

  return (
    <>
      <a className="skip-link" href="#conteudo" onClick={handleSkipLink}>Pular para o conteúdo</a>

      <div className="wiki-shell">
        <aside className="wiki-sidebar" aria-label="Navegação da documentação">
          <div className="brand-row">
            <a className="brand" href="#/sobre" aria-label="OpenCNPJ">
              <img src="./logo.svg?v=trim-1" alt="OpenCNPJ" className="logo" />
            </a>
          </div>

          <nav className="wiki-nav">
            {navigationItems.map((item) => {
              const isActive = isActiveGroup(item, activeRoute);

              return (
                <div className="wiki-nav-group" key={item.href ?? item.label}>
                  {item.href ? (
                    <a className={isActive ? 'active' : undefined} href={item.href} aria-current={isActive ? 'page' : undefined}>
                      {item.label}
                    </a>
                  ) : (
                    <span className={isActive ? 'wiki-nav-label active' : 'wiki-nav-label'}>{item.label}</span>
                  )}
                  {item.children ? (
                    <div className="wiki-nav-children">
                      {item.children.map((child) => {
                        const childActive = isActiveLink(child.href, activeRoute);

                        return (
                          <a className={childActive ? 'active' : undefined} key={child.href} href={child.href} aria-current={childActive ? 'page' : undefined}>
                            {child.label}
                          </a>
                        );
                      })}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </nav>

          <div className="sidebar-support" aria-label="Apoiar o OpenCNPJ">
            <p className="sidebar-support-text">Contribua para manter o OpenCNPJ online e em evolução.</p>
            <div className="sidebar-donation-actions">
              <a className="btn pix" href="https://link.mercadopago.com.br/opencnpj" target="_blank" rel="noopener" aria-label="Doar via Pix">
                <PixIcon />
                Pix
              </a>
              <a className="btn sponsor" href="https://github.com/sponsors/Hitmasu" target="_blank" rel="noopener">
                <HeartIcon />
                Sponsors
              </a>
            </div>
            <div className="sidebar-github">
              <GitHubStarsBadge />
            </div>
          </div>
        </aside>

        <main id="conteudo" className="wiki-content docs" tabIndex={-1}>
          {children}
        </main>
      </div>
    </>
  );
}
