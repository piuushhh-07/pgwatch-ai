import psycopg2
import requests
import click
from datetime import datetime, timedelta

# ── Connect to PostgreSQL ────────────────────────────
def get_connection(host, port, dbname, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        return conn
    except Exception as e:
        click.echo(f"[ERROR] Could not connect: {e}")
        return None

# ── Create fake pgwatch metrics table ───────────────
def setup_demo_tables(host, port, dbname, user, password):
    conn = get_connection(host, port, dbname, user, password)
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pgwatch_metrics (
                id SERIAL PRIMARY KEY,
                time TIMESTAMP DEFAULT NOW(),
                metric_name TEXT UNIQUE,
                metric_value FLOAT,
                details TEXT
            )
        """)
        cur.execute("""
            INSERT INTO pgwatch_metrics (metric_name, metric_value, details)
            VALUES 
            ('active_connections', 23, 'connections currently active'),
            ('lock_waits', 4, 'queries waiting for locks'),
            ('avg_query_time_ms', 4200, 'average query execution time'),
            ('checkpoint_warnings', 12, 'checkpoint warnings in bgwriter'),
            ('cache_hit_ratio', 94.5, 'buffer cache hit percentage'),
            ('deadlocks', 2, 'deadlocks detected'),
            ('idle_in_transaction', 3, 'connections idle in transaction')
            ON CONFLICT (metric_name) DO NOTHING
        """)
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        click.echo(f"[ERROR] Setup failed: {e}")
        return False

# ── Fetch Metrics ────────────────────────────────────
def fetch_metrics(host, port, dbname, user, password):
    conn = get_connection(host, port, dbname, user, password)
    if not conn:
        return {}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT metric_name, metric_value, details
            FROM pgwatch_metrics
            ORDER BY time DESC
        """)
        rows = cur.fetchall()
        metrics = {}
        for row in rows:
            metrics[row[0]] = {"value": row[1], "description": row[2]}
        cur.close()
        conn.close()
        return metrics
    except Exception as e:
        return {"error": str(e)}

# ── Build Prompt ─────────────────────────────────────
def build_prompt(question, metrics):
    metrics_text = ""
    for name, data in metrics.items():
        metrics_text += f"  - {name}: {data['value']} ({data['description']})\n"

    prompt = f"""You are a PostgreSQL database expert assistant for pgwatch.
A developer asked: {question}

Current database metrics:
{metrics_text}
Rules:
1. Only explain what the metrics actually show.
2. Give one concrete SQL query to investigate further.
3. Be direct and under 150 words.

Answer:"""
    return prompt

# ── Call Ollama (local LLM) ──────────────────────────
def call_llm(prompt):
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={"model": "llama3", "prompt": prompt, "stream": False},
            timeout=60
        )
        return response.json().get("response", "No response.")
    except Exception:
        return generate_simple_response(prompt)

# ── Simple response if no LLM available ─────────────
def generate_simple_response(prompt):
    if "slow" in prompt.lower() or "performance" in prompt.lower():
        return ("Based on metrics: avg query time is 4200ms which is very high. "
                "Lock waits (4) and idle_in_transaction (3) suggest blocking issues.\n\n"
                "Run this to find blocked queries:\n"
                "SELECT pid, now() - xact_start AS duration, state, query\n"
                "FROM pg_stat_activity WHERE state != 'idle'\n"
                "ORDER BY duration DESC;")
    elif "lock" in prompt.lower():
        return ("Metrics show 4 lock waits and 2 deadlocks. "
                "A long-running transaction is likely blocking others.\n\n"
                "Run: SELECT * FROM pg_locks WHERE granted = false;")
    else:
        return ("Current metrics show 23 active connections, "
                "4 lock waits, and avg query time of 4200ms. "
                "Cache hit ratio is 94.5% which is healthy.\n\n"
                "Run: SELECT * FROM pg_stat_activity WHERE state != 'idle';")

# ── Shared DB options decorator ──────────────────────
def db_options(f):
    f = click.option("--host",     default="localhost", show_default=True, help="DB host")(f)
    f = click.option("--port",     default=5432,        show_default=True, help="DB port")(f)
    f = click.option("--dbname",   default="postgres",  show_default=True, help="Database name")(f)
    f = click.option("--user",     default="postgres",  show_default=True, help="DB user")(f)
    f = click.option("--password", default="root",      show_default=True, help="DB password")(f)
    return f

# ── CLI ──────────────────────────────────────────────
@click.group()
def cli():
    """pgwatch-ai: Ask your PostgreSQL database what is wrong."""
    pass

@cli.command()
@click.argument("question")
@click.option("--dry-run", is_flag=True, help="Print prompt without calling LLM")
@db_options
def ask(question, dry_run, host, port, dbname, user, password):
    """Ask a natural language question about your database health."""
    click.echo(f"\n[pgwatch-ai] Connecting to PostgreSQL at {host}:{port}...")

    if not setup_demo_tables(host, port, dbname, user, password):
        click.echo("[pgwatch-ai] Could not connect. Check your credentials.")
        return

    click.echo(f"[pgwatch-ai] Fetching metrics...")
    metrics = fetch_metrics(host, port, dbname, user, password)

    if "error" in metrics:
        click.echo(f"[ERROR] {metrics['error']}")
        return

    click.echo(f"[pgwatch-ai] Found {len(metrics)} metrics:")
    for name, data in metrics.items():
        click.echo(f"  ✓ {name}: {data['value']}")

    prompt = build_prompt(question, metrics)

    if dry_run:
        click.echo("\n── DRY RUN: Prompt ──")
        click.echo(prompt)
        return

    click.echo("\n[pgwatch-ai] Analyzing...\n")
    click.echo("─" * 55)
    answer = call_llm(prompt)
    click.echo(answer)
    click.echo("─" * 55)

@cli.command()
@db_options
def report(host, port, dbname, user, password):
    """Generate a plain-text health summary of your database."""
    click.echo("\n[pgwatch-ai] Generating health report...\n")

    setup_demo_tables(host, port, dbname, user, password)
    metrics = fetch_metrics(host, port, dbname, user, password)

    click.echo("=" * 55)
    click.echo("        pgwatch-ai Health Report")
    click.echo(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    click.echo("=" * 55)

    warnings = []
    for name, data in metrics.items():
        val = data['value']
        if name == 'avg_query_time_ms' and val > 1000:
            status = "⚠️  HIGH"
            warnings.append(name)
        elif name == 'lock_waits' and val > 2:
            status = "⚠️  WARNING"
            warnings.append(name)
        elif name == 'cache_hit_ratio' and val < 90:
            status = "⚠️  LOW"
            warnings.append(name)
        else:
            status = "✅ OK"
        click.echo(f"  {status:12} {name}: {val}")

    click.echo("=" * 55)
    if warnings:
        click.echo(f"\n⚠️  Issues found: {', '.join(warnings)}")
    else:
        click.echo("\n✅ All metrics look healthy!")

if __name__ == "__main__":
    cli()