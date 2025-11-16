#!/usr/bin/env bash

# Simple colored logging helpers

_color() {
  local code="$1"; shift
  printf "\e[%sm%s\e[0m\n" "$code" "$*"
}

info() {
  _color "32" "[info] $*"
}

warn() {
  _color "33" "[warn] $*"
}

error() {
  _color "31" "[error] $*" >&2
}

