{{- define "k4a-mcp.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "k4a-mcp.fullname" -}}
{{- default .Chart.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "k4a-mcp.labels" -}}
app.kubernetes.io/name: {{ include "k4a-mcp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: k4a
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "k4a-mcp.selectorLabels" -}}
app.kubernetes.io/name: {{ include "k4a-mcp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "k4a-mcp.serviceAccountName" -}}
{{- default (include "k4a-mcp.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}
