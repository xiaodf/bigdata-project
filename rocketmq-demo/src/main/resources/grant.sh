#!/bin/bash
#username@host
#下面的语句作用是赋予103节点上的root用户对test数据库的select权限

for ip in `cat /etc/hosts | awk '{print $1}'`
do
    echo $ip
  mysql -e "grant select on test.* to root@$ip"
done
#赋权操作有个bug，因为有些执行rocfka的节点需要test数据库的insert权限
#grant select on test.* to root@172.16.8.103