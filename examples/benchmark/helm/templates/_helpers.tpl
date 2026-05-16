{{/*
Expand the name of the chart.
*/}}
{{- define "nats-grpc-benchmark.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "nats-grpc-benchmark.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "nats-grpc-benchmark.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Resolve a config value for a side ("server" or "client") with a fallback to
the .Values.common block.

Usage:
  {{ include "nats-grpc-benchmark.sideValue" (dict "ctx" . "side" "server" "key" "replicaCount") }}

Lookup order:
  1. .Values.<side>.<key>   (per-side override)
  2. .Values.common.<key>   (shared default)

Required so the chart can share replicaCount/count/payloadSize/resources/etc.
between client and server without duplicating them in values.yaml.
*/}}
{{- define "nats-grpc-benchmark.sideValue" -}}
{{- $ctx  := .ctx -}}
{{- $side := index $ctx.Values .side | default dict -}}
{{- $common := $ctx.Values.common | default dict -}}
{{- $key  := .key -}}
{{- $sideVal := index $side $key -}}
{{- if not (kindIs "invalid" $sideVal) -}}
{{- toYaml $sideVal -}}
{{- else -}}
{{- $commonVal := index $common $key -}}
{{- if not (kindIs "invalid" $commonVal) -}}
{{- toYaml $commonVal -}}
{{- else -}}
{{- fail (printf "nats-grpc-benchmark: missing required value %q for side %q (set %s.%s or common.%s)" $key .side .side $key $key) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Convenience wrapper that emits the value as a plain scalar (no surrounding
quotes/newlines). Use for things that appear inside YAML strings or command
flags, e.g. `--server-count={{ include "nats-grpc-benchmark.scalar" ... }}`.
*/}}
{{- define "nats-grpc-benchmark.scalar" -}}
{{- include "nats-grpc-benchmark.sideValue" . | trim -}}
{{- end -}}
