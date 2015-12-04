#!/bin/sh

#
# Set up master slave cluster for lhm specs
#

set -e
set -u

source `dirname $0`/lhm-config.sh

#
# Main
#


mkdir -p "$basedir/master/data" "$basedir/slave/data"

cat <<-CNF > $basedir/master/my.cnf
[mysqld]
log-bin = master-bin
log-bin-index = master-bin.index
innodb_large_prefix = 1
innodb_file_format = Barracuda
innodb_file_per_table = 1
pid-file = $basedir/master/mysqld.pid
socket = $basedir/master/mysqld.sock
port = $master_port
log_output = FILE
log-error = $basedir/master/error.log
datadir = $basedir/master/data
server-id = 1
CNF

cat <<-CNF > $basedir/slave/my.cnf
[mysqld]
innodb_large_prefix = 1
innodb_file_format = Barracuda
innodb_file_per_table = 1
pid-file = $basedir/slave/mysqld.pid
socket = $basedir/slave/mysqld.sock
port = $slave_port
log_output = FILE
log-error = $basedir/slave/error.log
datadir = $basedir/slave/data
relay-log = slave-relay-bin
relay-log-index = slave-relay-bin.index
server-id = 2

# replication (optional filters)

# replicate-do-table = lhm.users
# replicate-do-table = lhm.lhmn_users
# replicate-wild-do-table = lhm.lhma_%_users

# replicate-do-table = lhm.origin
# replicate-do-table = lhm.lhmn_origin
# replicate-wild-do-table = lhm.lhma_%_origin

# replicate-do-table = lhm.destination
# replicate-do-table = lhm.lhmn_destination
# replicate-wild-do-table = lhm.lhma_%_destination
CNF

# build system tables

(
  cd "$mysqldir"
  install_bin="$(echo ./*/mysql_install_db | tr " " "\\n" | head -1)"
  $install_bin --datadir="$basedir/master/data"
  $install_bin --datadir="$basedir/slave/data"
)
