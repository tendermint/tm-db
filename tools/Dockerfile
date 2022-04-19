# This file defines the container image used to build and test tm-db in CI.
# The CI workflows use the latest tag of tendermintdev/docker-tm-db-testing
# built from these settings.
#
# The jobs defined in the Build & Push workflow will build and update the image
# when changes to this file are merged.  If you have other changes that require
# updates here, merge the changes here first and let the image get updated (or
# push a new version manually) before PRs that depend on them.

FROM faddat/archlinux

RUN pacman -Syyu --noconfirm rocksdb leveldb go git base-devel