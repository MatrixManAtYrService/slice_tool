#! /usr/bin/env bash
mysql -uroot -ptest -t -e "source sql/show_everything.sql;" | sed 's/^/    /g'
