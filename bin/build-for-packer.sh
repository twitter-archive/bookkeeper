#!/bin/bash

set -x

CWD=$(pwd)

echo "${CWD}"
java -version


mvn -Pnative -Pzk3.4 -Ptwitter-science-provider clean package -DskipTests || exit 1

dist="dist"
mkdir -p "${dist}" "${dist}/conf" "${dist}/target" "${dist}/bin" "${dist}/lib"

cp bookkeeper-server/lib/* "${dist}/lib/"
cp bookkeeper-server/bin/bookkeeper "${dist}/bin"
cp bookkeeper-server/conf/* "${dist}/conf/"

echo "#!/bin/bash

java -cp target/*:lib/* org.apache.bookkeeper.bookie.BookieShell -conf ./conf/bk_server.conf $@" > "${dist}/bkshell"

chmod +x "${dist}/bkshell"

mkdir -p "${dist}/lib/native"

bkversion=$(grep "version>.*</version" bookkeeper-server/pom.xml  | awk -F '[><]' '{print $3}'|head -1)

cp "bookkeeper-server/target/bookkeeper-server-${bkversion}.jar" "${dist}/target"

if [ -f "bookkeeper-server/target/native/target/usr/local/lib/libbookkeeper.so.1.0.0" ]; then
  cp "bookkeeper-server/target/native/target/usr/local/lib/libbookkeeper.so.1.0.0" "$dist/lib/native/libbookkeeper.so.1.0.0"
  ln -s bookkeeper-server/lib/native/libbookkeeper.so.1.0.0 "$dist/lib/native/libbookkeeper.so"
fi

pushd "${dist}"
chmod +x "bin/bookkeeper"

zip -r bookkeeper-server-deploy.zip .

if [[ ! -f bookkeeper-server-deploy.zip ]] || [[ ! -s bookkeeper-server-deploy.zip ]]; then
  echo "failed to build the artifact"
  exit 1
fi

popd
