#!/bin/bash
CLIENT=(include/libbmobject-store.h include/librados-mobject-store.h include/mobject-client.h src/client src/omap-iter)
SERVER=(include/mobject-server.h src/io-chain src/io-chain src/rpc-types src/server src/util)
echo "************** CLIENT ***************"
sloccount "${CLIENT[@]}"

echo "************** SERVER ***************"
sloccount "${SERVER[@]}"
