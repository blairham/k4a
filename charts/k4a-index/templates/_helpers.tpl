{{- define "k4a-index.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified name, the standard Helm way: the release name, plus the chart
name unless the release name already contains it. So a release named k4a-index
keeps the bare name "k4a-index", and two releases in one namespace no longer
collide. fullnameOverride wins outright.
*/}}
{{- define "k4a-index.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "k4a-index.labels" -}}
app.kubernetes.io/name: {{ include "k4a-index.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: k4a
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "k4a-index.selectorLabels" -}}
app.kubernetes.io/name: {{ include "k4a-index.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "k4a-index.serviceAccountName" -}}
{{- default (include "k4a-index.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}
