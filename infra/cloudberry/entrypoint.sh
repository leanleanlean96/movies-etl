#!/usr/bin/env bash
set -euo pipefail

if [[ ! "${CLOUDBERRY_PASSWORD:-}" =~ ^[A-Za-z0-9]+$ ]]; then
  echo 'CLOUDBERRY_PASSWORD must contain only letters and numbers' >&2
  exit 1
fi

mkdir -p /run/sshd
ssh-keygen -A >/dev/null
/usr/sbin/sshd
chown gpadmin:gpadmin /var/lib/cloudberry

if [[ ! -f /home/gpadmin/.ssh/id_rsa ]]; then
  install -d -m 700 -o gpadmin -g gpadmin /home/gpadmin/.ssh
  su - gpadmin -c 'ssh-keygen -q -t rsa -b 3072 -N "" -f ~/.ssh/id_rsa'
  cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys
  chown gpadmin:gpadmin /home/gpadmin/.ssh/authorized_keys
  chmod 600 /home/gpadmin/.ssh/authorized_keys
fi

# Host keys are regenerated when the container is recreated (/etc/ssh is not in a volume).
# gpstart/gpstop ssh to this same host, so a stale known_hosts entry must not block them.
cat > /home/gpadmin/.ssh/config <<SSHCFG
Host localhost 127.0.0.1 cloudberry $(hostname)
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  LogLevel ERROR
SSHCFG
chown gpadmin:gpadmin /home/gpadmin/.ssh/config
chmod 600 /home/gpadmin/.ssh/config

if [[ ! -f /var/lib/cloudberry/gpdemo-env.sh ]]; then
  su - gpadmin -c 'source /usr/local/cloudberry-db/cloudberry-env.sh && cd /var/lib/cloudberry && PORT_BASE=7000 NUM_PRIMARY_MIRROR_PAIRS=2 WITH_MIRRORS=false gpdemo'
  su - gpadmin -c 'source /usr/local/cloudberry-db/cloudberry-env.sh && source /var/lib/cloudberry/gpdemo-env.sh && psql -d postgres -c "CREATE DATABASE etl"'
else
  su - gpadmin -c 'source /usr/local/cloudberry-db/cloudberry-env.sh && source /var/lib/cloudberry/gpdemo-env.sh && gpstart -a'
fi

su - gpadmin -c "source /usr/local/cloudberry-db/cloudberry-env.sh && source /var/lib/cloudberry/gpdemo-env.sh && psql -d postgres -c \"ALTER ROLE gpadmin PASSWORD '${CLOUDBERRY_PASSWORD}'\""
su - gpadmin -c 'source /usr/local/cloudberry-db/cloudberry-env.sh && source /var/lib/cloudberry/gpdemo-env.sh && sed -i "/^# ETL local access$/,/^# end ETL local access$/d" "${COORDINATOR_DATA_DIRECTORY}/postgresql.conf" && sed -i "/^# ETL local access$/,/^# end ETL local access$/d" "${COORDINATOR_DATA_DIRECTORY}/pg_hba.conf" && printf "\n# ETL local access\nlisten_addresses = '\''*'\''\n# end ETL local access\n" >> "${COORDINATOR_DATA_DIRECTORY}/postgresql.conf" && printf "\n# ETL local access\nhost all all 0.0.0.0/0 scram-sha-256\n# end ETL local access\n" >> "${COORDINATOR_DATA_DIRECTORY}/pg_hba.conf" && gpstop -ra'

shutdown() {
  su - gpadmin -c 'source /usr/local/cloudberry-db/cloudberry-env.sh && source /var/lib/cloudberry/gpdemo-env.sh && gpstop -a' || true
  exit 0
}
trap shutdown TERM INT
while true; do sleep 3600 & wait $!; done
