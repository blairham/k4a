{{- define "k4a-index.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "k4a-index.fullname" -}}
{{- default .Chart.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
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
