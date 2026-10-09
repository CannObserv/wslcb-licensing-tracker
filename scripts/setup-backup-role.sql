-- The nightly backup's database role (#185). Ported from CannObserv/watcher's
-- scripts/setup-backup-role.sql (watcher#297).
--
-- READ THIS BEFORE RUNNING IT. It runs as the superuser against the live
-- `wslcb` database. It is purely additive: one role, which can read and do
-- nothing else.
--
--   sudo -u postgres psql -d wslcb < scripts/setup-backup-role.sql
--
-- Redirected, not `-f`: the `postgres` OS user cannot read anything under
-- /home/exedev (0750). The redirect is performed by the invoking shell, which can.
--
--
-- WHAT IT DOES
--
--   wslcb_backup    LOGIN, no password, a member of pg_read_all_data - SELECT
--                   on every table and sequence and USAGE on every schema,
--                   which is everything pg_dump reads and all it can do.
--                   Cluster-wide, though: it reads in every database the role
--                   may CONNECT to, and PUBLIC may connect to any by default.
--                   On this cluster that is `wslcb` and `wslcb_test`.
--                   Reached only by peer auth over the local socket (pg_hba's
--                   `local all all peer`) from the OS user of the same name:
--                   the dynamic user infra/wslcb-backup.service allocates
--                   for each run, which exists only while the job does.
--
-- ONCE PER CLUSTER, before the backup's first run. A role is cluster state,
-- which no dump carries, so a host restored from a dump needs it again. The
-- runbook is docs/RECOVERY.md.
--
-- IDEMPOTENT. Re-running re-asserts the role's attributes and grants.

\set ON_ERROR_STOP on

-- A psql variable so a rehearsal can run these exact bytes with
-- -v backup_role=... . It must equal the unit's User=: peer auth maps the OS
-- user to the role of the same name.
\if :{?backup_role}
\else
  \set backup_role wslcb_backup
\endif

BEGIN;

SELECT format('CREATE ROLE %I LOGIN', :'backup_role')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'backup_role');
\gexec

-- Re-asserted every run, so a role that acquired an attribute by hand loses it
-- again. NOBYPASSRLS keeps a table under row security a loud pg_dump failure
-- rather than a dump of only the rows a policy shows. PASSWORD NULL: no
-- password rule in pg_hba can ever admit it.
ALTER ROLE :"backup_role" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS INHERIT PASSWORD NULL;

-- WITH INHERIT TRUE, explicitly: pg_read_all_data's rights reach the role only
-- through an inheriting membership, and on PostgreSQL 16 a re-grant that omits
-- the option keeps whatever the existing membership has.
GRANT pg_read_all_data TO :"backup_role" WITH INHERIT TRUE;

-- Already granted to PUBLIC on this cluster; stated so a later REVOKE ... FROM
-- PUBLIC hardening pass does not silently stop the backups.
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'backup_role');
\gexec

COMMIT;

-- Report. Read-only. The role row must read t for login, inherit,
-- no_password, reads_all_data and can_connect, and f for every other column.
-- Every count below it must be 0: a relation the role cannot read, a table
-- under row security, or a large object (pg_read_all_data covers tables, views
-- and sequences - not large objects) each fails the nightly pg_dump.
SELECT
  r.rolname,
  r.rolcanlogin AS login,
  r.rolinherit AS inherit,
  r.rolsuper AS superuser,
  r.rolcreatedb AS createdb,
  r.rolcreaterole AS createrole,
  r.rolreplication AS replication,
  r.rolbypassrls AS bypassrls,
  a.rolpassword IS NULL AS no_password,
  pg_has_role(r.rolname, 'pg_read_all_data', 'USAGE') AS reads_all_data,
  has_database_privilege(r.rolname, current_database(), 'CONNECT') AS can_connect
FROM pg_roles r
JOIN pg_authid a ON a.oid = r.oid
WHERE r.rolname = :'backup_role';

SELECT
  (SELECT count(*)
     FROM pg_class c
     JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p', 'S', 'm')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname NOT LIKE 'pg_toast%'
      AND NOT has_table_privilege(:'backup_role', c.oid, 'SELECT')) AS unreadable,
  (SELECT count(*) FROM pg_class WHERE relrowsecurity) AS under_row_security,
  (SELECT count(*) FROM pg_largeobject_metadata) AS large_objects;
