#!/bin/sh

set -e

KERBEROS=${KERBEROS:-false}
AES=${AES:-false}
RPC_PROTECTION=${RPC_PROTECTION:-authentication}
DATA_TRANSFER_PROTECTION=${TRANSFER_PROTECTION:-}
if [ "$DATA_TRANSFER_PROTECTION" = "privacy" ]; then
  KERBEROS="true"
  ENCRYPT_DATA_TRANSFER="true"
  ENCRYPT_DATA_TRANSFER_ALG="rc4"
  if [ "$AES" = "true" ]; then
      ENCRYPT_DATA_TRANSFER_CIPHER="AES/CTR/NoPadding"
  fi
else
  ENCRYPT_DATA_TRANSFER="false"
fi

CONF_AUTHENTICATION="simple"
KERBEROS_REALM="EXAMPLE.COM"
KERBEROS_PRINCIPLE="administrator"
KERBEROS_PASSWORD="password1234"
if [ "$KERBEROS" = "true" ]; then
  CONF_AUTHENTICATION="kerberos"

  HOSTNAME=$(hostname)

  sudo tee /etc/krb5.conf << EOF
[libdefaults]
    default_realm = $KERBEROS_REALM
    dns_lookup_realm = false
    dns_lookup_kdc = false
[realms]
    $KERBEROS_REALM = {
        kdc = localhost
        admin_server = localhost
    }
[logging]
    default = FILE:/var/log/krb5libs.log
    kdc = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmind.log
[domain_realm]
    .localhost = $KERBEROS_REALM
    localhost = $KERBEROS_REALM
EOF

  sudo mkdir /etc/krb5kdc
  sudo printf '*/*@%s\t*' "$KERBEROS_REALM" | sudo tee /etc/krb5kdc/kadm5.acl

  sudo apt-get update
  sudo apt-get install -y krb5-user krb5-kdc krb5-admin-server

  printf "$KERBEROS_PASSWORD\n$KERBEROS_PASSWORD" | sudo kdb5_util -r "$KERBEROS_REALM" create -s
  for p in nn dn $USER gohdfs1 gohdfs2; do
    sudo kadmin.local -q "addprinc -randkey $p/$HOSTNAME@$KERBEROS_REALM"
    sudo kadmin.local -q "addprinc -randkey $p/localhost@$KERBEROS_REALM"
    sudo kadmin.local -q "xst -k /tmp/$p.keytab $p/$HOSTNAME@$KERBEROS_REALM"
    sudo kadmin.local -q "xst -k /tmp/$p.keytab $p/localhost@$KERBEROS_REALM"
    sudo chmod +rx /tmp/$p.keytab
  done

  echo "Restarting krb services..."
  sudo service krb5-kdc restart
  sudo service krb5-admin-server restart

  kinit -kt /tmp/$USER.keytab "$USER/localhost@$KERBEROS_REALM"

  # The go tests need ccache files for these principles in a specific place.
  for p in $USER gohdfs1 gohdfs2; do
    kinit -kt "/tmp/$p.keytab" -c "/tmp/krb5cc_gohdfs_$p" "$p/localhost@$KERBEROS_REALM"
  done
fi

case "$HADOOP_VERSION" in
  2.10.1)
    SHA512=2460e02cd1f80dfed7a8981bbc934c095c0a341435118bec781fd835ec2ebdc5543a03d92d24f2ddeebdfe1c2c460065ba1d394ed9a73cbb2020b40a8d8b5e07
    ;;
  3.3.1)
    SHA512=2fd0bf74852c797dc864f373ec82ffaa1e98706b309b30d1effa91ac399b477e1accc1ee74d4ccbb1db7da1c5c541b72e4a834f131a99f2814b030fbd043df66
    ;;
  *)
    echo "Unverified Hadoop version: $HADOOP_VERSION" >&2
    exit 1
    ;;
esac
URL="https://repo.huaweicloud.com/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
echo "Downloading $URL"
curl --fail --location --retry 3 --connect-timeout 30 --max-time 900 -o hadoop.tar.gz "$URL"
printf '%s  %s\n' "$SHA512" hadoop.tar.gz | sha512sum --check -
tar zxf hadoop.tar.gz

HADOOP_ROOT="hadoop-${HADOOP_VERSION}/"
mkdir -p /tmp/hdfs/name /tmp/hdfs/data

sudo tee $HADOOP_ROOT/etc/hadoop/core-site.xml <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.security.authentication</name>
    <value>$CONF_AUTHENTICATION</value>
  </property>
  <property>
    <name>hadoop.security.authorization</name>
    <value>$KERBEROS</value>
  </property>
  <property>
    <name>dfs.namenode.keytab.file</name>
    <value>/tmp/nn.keytab</value>
  </property>
  <property>
    <name>dfs.namenode.kerberos.principal</name>
    <value>nn/localhost@$KERBEROS_REALM</value>
  </property>
  <property>
    <name>dfs.web.authentication.kerberos.principal</name>
    <value>nn/localhost@$KERBEROS_REALM</value>
  </property>
  <property>
    <name>dfs.datanode.keytab.file</name>
    <value>/tmp/dn.keytab</value>
  </property>
  <property>
    <name>dfs.datanode.kerberos.principal</name>
    <value>dn/localhost@$KERBEROS_REALM</value>
  </property>
  <property>
    <name>hadoop.rpc.protection</name>
    <value>$RPC_PROTECTION</value>
  </property>
</configuration>
EOF

sudo tee $HADOOP_ROOT/etc/hadoop/hdfs-site.xml <<EOF
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/tmp/hdfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/tmp/hdfs/data</value>
  </property>
  <property>
   <name>dfs.permissions.superusergroup</name>
   <value>hadoop</value>
  </property>
  <property>
    <name>dfs.safemode.extension</name>
    <value>0</value>
  </property>
  <property>
     <name>dfs.safemode.min.datanodes</name>
     <value>1</value>
  </property>
  <property>
    <name>dfs.block.access.token.enable</name>
    <value>$KERBEROS</value>
  </property>
  <property>
    <name>ignore.secure.ports.for.testing</name>
    <value>true</value>
  </property>
  <property>
    <name>dfs.data.transfer.protection</name>
    <value>$DATA_TRANSFER_PROTECTION</value>
  </property>
  <property>
    <name>dfs.encrypt.data.transfer</name>
    <value>$ENCRYPT_DATA_TRANSFER</value>
  </property>
  <property>
    <name>dfs.encrypt.data.transfer.algorithm</name>
    <value>$ENCRYPT_DATA_TRANSFER_ALG</value>
  </property>
  <property>
    <name>dfs.encrypt.data.transfer.cipher.suites</name>
    <value>$ENCRYPT_DATA_TRANSFER_CIPHER</value>
  </property>
</configuration>
EOF

$HADOOP_ROOT/bin/hdfs namenode -format
sudo groupadd hadoop
sudo usermod -a -G hadoop $USER

echo "Starting namenode..."
$HADOOP_ROOT/bin/hdfs namenode > /tmp/hdfs/namenode.log 2>&1 &

echo "Starting datanode..."
$HADOOP_ROOT/bin/hdfs datanode > /tmp/hdfs/datanode.log 2>&1 &

sleep 5

echo "Waiting for cluster to exit safe mode..."
$HADOOP_ROOT/bin/hdfs dfsadmin -safemode wait

echo "HADOOP_CONF_DIR=$(pwd)/$HADOOP_ROOT/etc/hadoop" >> $GITHUB_ENV
echo "$(pwd)/$HADOOP_ROOT/bin" >> $GITHUB_PATH
