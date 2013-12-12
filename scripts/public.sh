#!/bin/bash

PROJECT=swift
CODE=git://github.com/openstack/swift.git 
apt-get install -y git git-core
git clone $CODE /opt/$PROJECT

apt-get install -y curl gcc libffi-dev python-setuptools
apt-get install -y python-dev python-pip
pip install -r /opt/$PROJECT/requirements.txt

mkdir -p /var/cache/swift /srv/node/
mkdir -p /etc/swift/

cat >/etc/swift/swift.conf <<EOF
[swift-hash]
swift_hash_path_suffix = randomestringchangeme
EOF

apt-get install -y rsyslog
mkdir -p /var/log/swift
chown -R syslog.adm /var/log/swift
cat >/etc/rsyslog.d/10-swift.conf<<EOF
local1,local2,local3,local4.*   /var/log/swift/all.log

local1.*;local1.!notice        /var/log/swift/object.log
local1.notice                /var/log/swift/object.error
local1.*                ~
        
local2.*;local2.!notice        /var/log/swift/container.log
local2.notice                /var/log/swift/container.error
local2.*                ~

local3.*;local3.!notice        /var/log/swift/account.log
local3.notice                /var/log/swift/account.error
local3.*

local4.*;local4.!notice        /var/log/swift/proxy.log
local4.notice                /var/log/swift/proxy.error
local4.*
EOF
