#!/bin/sh

sudo sh -c 'echo "127.0.0.1 tt.btplug.dongxiguo.com" >> /etc/hosts'

sudo sed -i -e 's/-p 11211/-p 1978/' /etc/memcached.conf

sudo service memcached restart
