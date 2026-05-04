import os
from pathlib import Path

from dotenv import load_dotenv


def _clean_db_url(url: str) -> str:
    """
    Убирает channel_binding=require, который не поддерживает psycopg2.
    (Скопировано по смыслу из app.py, чтобы миграции применялись тем же URL.)
    """
    import urllib.parse as urlparse

    parsed = urlparse.urlparse(url)
    qs = urlparse.parse_qs(parsed.query)
    qs.pop("channel_binding", None)
    new_q = urlparse.urlencode({k: v[0] for k, v in qs.items()})
    return urlparse.urlunparse(parsed._replace(query=new_q))


def main() -> int:
    load_dotenv()

    db_url = os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        raise SystemExit(
            "DATABASE_URL не задан. Создайте файл .env рядом с app.py "
            "или установите переменную окружения DATABASE_URL."
        )

    try:
        import psycopg2
    except Exception as e:
        raise SystemExit(
            "Не найден psycopg2. Установите зависимости: pip install -r requirements.txt"
        ) from e

    migrations_dir = Path(__file__).resolve().parent / "migrations"
    if not migrations_dir.exists():
        raise SystemExit(f"Не найдена папка миграций: {migrations_dir}")

    sql_files = sorted(p for p in migrations_dir.iterdir() if p.suffix.lower() == ".sql")
    if not sql_files:
        raise SystemExit(f"В папке миграций нет .sql файлов: {migrations_dir}")

    conn = psycopg2.connect(_clean_db_url(db_url), connect_timeout=20)
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              filename TEXT PRIMARY KEY,
              applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        conn.commit()

        for path in sql_files:
            filename = path.name
            cur.execute(
                "SELECT 1 FROM schema_migrations WHERE filename = %s",
                (filename,),
            )
            if cur.fetchone():
                print(f"[skip] {filename}")
                continue

            sql = path.read_text(encoding="utf-8")
            print(f"[apply] {filename}")
            try:
                cur.execute(sql)
                cur.execute(
                    "INSERT INTO schema_migrations(filename) VALUES (%s)",
                    (filename,),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    conn.close()
    print("OK: миграции применены.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

