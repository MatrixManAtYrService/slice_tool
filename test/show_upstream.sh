#! /usr/bin/env bash
mysql -uroot -ptest -e "use things_upstream; source sql/show_one_side.sql;" | sed 's/^/    /g'
