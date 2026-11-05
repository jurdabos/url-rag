#!/usr/bin/env bash
# Loads the alert sender from the WSL keyring into Airflow's connection store, so the
# Gmail app password never sits in a plain file. Containers cannot reach the keyring,
# which stays the durable copy. Rerun after `astro dev kill`, which wipes Airflow's
# database; `astro dev restart` keeps it. The keyring prompts for its master password.
# It ends by sending a test email to the `rag_alert_email` Variable.
#
# Usage: GMAIL_2_USER=REALADDRESS bash scripts/set_smtp_connection.sh
set -euo pipefail

: "${GMAIL_2_USER:?Set GMAIL_2_USER to the sending address}"
scheduler=$(docker ps --format '{{.Names}}' --filter 'name=scheduler' | grep -m1 '^url-rag_') || {
  echo "No running url-rag scheduler: start the stack first." >&2
  exit 1
}
password=$(keyring get email gmail_2_password)
if [ -z "$password" ]; then
  echo "No gmail_2_password under the email service in the keyring." >&2
  exit 1
fi
# The password travels over stdin, so it never appears in a process list
printf '%s' "$password" | docker exec -i "$scheduler" python -c '
import json
import sys

from airflow.models.connection import Connection
from airflow.utils.session import create_session

user = sys.argv[1]
password = sys.stdin.read().strip().replace(" ", "")
# Port 465 is encrypted from the start and offers no STARTTLS, which the hook would otherwise call.
# So disable_tls skips that upgrade only: the connection stays encrypted.
with create_session() as session:
    session.query(Connection).filter(Connection.conn_id == "smtp_default").delete()
    session.add(
        Connection(
            conn_id="smtp_default",
            conn_type="smtp",
            host="smtp.gmail.com",
            port=465,
            login=user,
            password=password,
            extra=json.dumps({"from_email": user, "disable_tls": True}),
        )
    )
print("Stored the smtp_default connection")
' "$GMAIL_2_USER" 2>/dev/null | tail -1
# Sending a real message, because a wrong password would otherwise only show up when an alert fires
docker exec -i "$scheduler" python - 2>&1 <<'PY' | tail -1
import os

from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.utils.session import create_session

# Outside a task the hook cannot read the database, so this process hands it the stored connection
with create_session() as session:
    stored = session.query(Connection).filter(Connection.conn_id == "smtp_default").one()
    os.environ["AIRFLOW_CONN_SMTP_DEFAULT"] = stored.get_uri()
recipient = Variable.get("rag_alert_email")
with SmtpHook(smtp_conn_id="smtp_default") as hook:
    hook.send_email_smtp(to=recipient, subject="[url-rag] test alert", html_content="<pre>Mail setup works.</pre>")
print(f"Sent a test email to {recipient}")
PY
