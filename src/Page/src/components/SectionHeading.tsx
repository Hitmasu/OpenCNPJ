interface SectionHeadingProps {
  title: string;
  level?: 1 | 2;
}

export function SectionHeading({ title, level = 2 }: SectionHeadingProps) {
  const HeadingTag = level === 1 ? 'h1' : 'h2';

  return (
    <div className="section-heading">
      <HeadingTag>{title}</HeadingTag>
    </div>
  );
}
