{{/* Chart name, truncated to the 63 characters a DNS label allows. */}}
{{- define "dbt-temporal.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Fully qualified app name: the release name, prefixed with the chart name unless it already contains it. */}}
{{- define "dbt-temporal.fullname" -}}
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

{{- define "dbt-temporal.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "dbt-temporal.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dbt-temporal.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "dbt-temporal.labels" -}}
helm.sh/chart: {{ include "dbt-temporal.chart" . }}
{{ include "dbt-temporal.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: worker
{{- end }}

{{- define "dbt-temporal.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dbt-temporal.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Image reference: tag defaults to appVersion; the distroless variant is a tag suffix. */}}
{{- define "dbt-temporal.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- if eq .Values.image.variant "distroless" }}
{{- $tag = printf "%s-distroless" $tag }}
{{- else if .Values.image.variant }}
{{- fail (printf "image.variant must be \"\" or \"distroless\", got %q" .Values.image.variant) }}
{{- end }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/* Name of the Secret that holds profiles.yml, or empty when none is configured. */}}
{{- define "dbt-temporal.profilesSecret" -}}
{{- if .Values.dbt.profiles.existingSecret }}
{{- .Values.dbt.profiles.existingSecret }}
{{- else if .Values.dbt.profiles.inline }}
{{- printf "%s-profiles" (include "dbt-temporal.fullname" .) }}
{{- end }}
{{- end }}

{{/* Whether a metrics Service is rendered: asked for directly, or needed by a ServiceMonitor. */}}
{{- define "dbt-temporal.metricsService" -}}
{{- if and .Values.metrics.enabled (or .Values.metrics.service.enabled .Values.metrics.serviceMonitor.enabled) }}true{{ end }}
{{- end }}

{{/*
terminationGracePeriodSeconds: an explicit value wins; otherwise the drain
window plus a buffer for cancelled queries to stop. With no drain window the
worker waits for every activity however long it runs, and no derived pod
deadline would honour that — so the chart refuses to pick one.
*/}}
{{- define "dbt-temporal.terminationGracePeriodSeconds" -}}
{{- $w := .Values.worker }}
{{- if include "dbt-temporal.isSet" $w.terminationGracePeriodSeconds }}
{{- $w.terminationGracePeriodSeconds | int }}
{{- else if include "dbt-temporal.isSet" $w.gracefulShutdownSeconds }}
{{- add ($w.gracefulShutdownSeconds | int) ($w.terminationGraceBufferSeconds | int) }}
{{- else }}
{{- fail "worker.gracefulShutdownSeconds is unset, so the worker drains in-flight activities with no deadline: set worker.terminationGracePeriodSeconds to how long Kubernetes may wait before SIGKILL" }}
{{- end }}
{{- end }}

{{/*
Non-empty when a scalar value is set. Numbers arrive as float64 from values
files and int64 from --set, so this tests for absence rather than a kind.
*/}}
{{- define "dbt-temporal.isSet" -}}
{{- if and (not (kindIs "invalid" .)) (ne (toString .) "") }}true{{ end }}
{{- end }}
