import { PrismLight as SyntaxHighlighter } from 'react-syntax-highlighter';
import http from 'react-syntax-highlighter/dist/esm/languages/prism/http';
import sql from 'react-syntax-highlighter/dist/esm/languages/prism/sql';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';

SyntaxHighlighter.registerLanguage('http', http);
SyntaxHighlighter.registerLanguage('sql', sql);

interface CodeBlockProps {
  code: string;
  language: 'http' | 'sql';
  label?: string;
}

export function CodeBlock({ code, language, label }: CodeBlockProps) {
  return (
    <figure className="code-block">
      <figcaption>{label ?? language.toUpperCase()}</figcaption>
      <div className="code-block-body">
        <SyntaxHighlighter
          codeTagProps={{ className: 'code-block-code' }}
          customStyle={{ margin: 0, padding: 0, background: 'transparent' }}
          language={language}
          PreTag="div"
          style={vscDarkPlus}
        >
          {code.trim()}
        </SyntaxHighlighter>
      </div>
    </figure>
  );
}
