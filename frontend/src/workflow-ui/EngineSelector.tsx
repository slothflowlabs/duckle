import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { createPortal } from 'react-dom';
import { Check, ChevronDown } from 'lucide-react';

export type EngineId = 'duckdb' | 'slothdb' | 'native';

type EngineMeta = {
    id: EngineId;
    label: string;
    description: string;
    dot: string;
    /** Not selectable yet - shown greyed with a "coming soon" note. */
    comingSoon?: boolean;
};

const ENGINES: EngineMeta[] = [
    {
        id: 'duckdb',
        label: 'DuckDB',
        description: 'Default. Local analytics, files, SQL pushdown.',
        dot: '#fff100',
    },
    {
        id: 'slothdb',
        label: 'SlothDB',
        description: 'Optional embedded analytics engine.',
        dot: '#3d8bff',
    },
    {
        id: 'native',
        label: 'Native',
        description: 'Rust streaming and incremental pipelines.',
        dot: '#2eafff',
        comingSoon: true,
    },
];

type Props = {
    value: EngineId;
    onChange: (id: EngineId) => void;
};

export default function EngineSelector({ value, onChange }: Props) {
    const { t } = useTranslation();
    const [open, setOpen] = useState(false);
    const [position, setPosition] = useState<{ top: number; left: number; width: number } | null>(
        null,
    );
    const triggerRef = useRef<HTMLButtonElement>(null);
    const dropdownRef = useRef<HTMLDivElement>(null);
    const current = ENGINES.find(e => e.id === value) ?? ENGINES[0]!;

    useLayoutEffect(() => {
        if (!open) return;
        const update = () => {
            const el = triggerRef.current;
            if (!el) return;
            const rect = el.getBoundingClientRect();
            setPosition({
                top: rect.bottom + 6,
                left: rect.left,
                width: Math.max(rect.width, 320),
            });
        };
        update();
        window.addEventListener('resize', update);
        window.addEventListener('scroll', update, true);
        return () => {
            window.removeEventListener('resize', update);
            window.removeEventListener('scroll', update, true);
        };
    }, [open]);

    useEffect(() => {
        if (!open) return;
        const handleClick = (e: MouseEvent) => {
            const target = e.target as Node;
            if (
                triggerRef.current &&
                !triggerRef.current.contains(target) &&
                dropdownRef.current &&
                !dropdownRef.current.contains(target)
            ) {
                setOpen(false);
            }
        };
        const handleKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') setOpen(false);
        };
        document.addEventListener('mousedown', handleClick);
        document.addEventListener('keydown', handleKey);
        return () => {
            document.removeEventListener('mousedown', handleClick);
            document.removeEventListener('keydown', handleKey);
        };
    }, [open]);

    return (
        <div className="engine-selector">
            <button
                ref={triggerRef}
                type="button"
                className="engine-trigger"
                aria-haspopup="listbox"
                aria-expanded={open}
                onClick={() => setOpen(o => !o)}
            >
                <span className="engine-dot" style={{ background: current.dot }} aria-hidden />
                <span className="engine-trigger-label">{t('engine.label')}</span>
                <span className="engine-trigger-value">{current.label}</span>
                <ChevronDown size={12} className="engine-trigger-chevron" aria-hidden="true" />
            </button>
            {open && position
                ? createPortal(
                      <div
                          ref={dropdownRef}
                          className="engine-dropdown engine-dropdown-portal"
                          role="listbox"
                          aria-label="Engine"
                          style={{
                              top: position.top,
                              left: position.left,
                              minWidth: position.width,
                          }}
                      >
                          {ENGINES.map(e => (
                              <button
                                  key={e.id}
                                  type="button"
                                  role="option"
                                  aria-selected={e.id === value}
                                  aria-disabled={e.comingSoon}
                                  disabled={e.comingSoon}
                                  className={
                                      'engine-option' +
                                      (e.comingSoon ? ' is-coming-soon' : '')
                                  }
                                  onClick={() => {
                                      if (e.comingSoon) return;
                                      onChange(e.id);
                                      setOpen(false);
                                  }}
                              >
                                  <span
                                      className="engine-dot"
                                      style={{ background: e.dot }}
                                      aria-hidden
                                  />
                                  <div className="engine-option-text">
                                      <div className="engine-option-label">
                                          {e.label}
                                          {e.comingSoon ? (
                                              <span className="engine-option-soon">
                                                  {t('engine.comingSoon')}
                                              </span>
                                          ) : null}
                                      </div>
                                      <div className="engine-option-desc">{e.description}</div>
                                  </div>
                                  {e.id === value && !e.comingSoon ? (
                                      <Check
                                          size={14}
                                          className="engine-option-check"
                                          aria-hidden="true"
                                      />
                                  ) : null}
                              </button>
                          ))}
                      </div>,
                      document.body,
                  )
                : null}
        </div>
    );
}
