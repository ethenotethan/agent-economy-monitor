resource "google_service_account" "scheduler" {
  account_id   = "agent-economy-scheduler"
  display_name = "Agent Economy Monitor scheduler"
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_job_runner" {
  for_each = local.worker_modes

  project  = var.project_id
  name     = google_cloud_run_v2_job.worker[each.key].name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "worker" {
  for_each = local.worker_modes

  name             = "agent-economy-${each.key}"
  description      = "Invoke the bounded ${each.key} worker job"
  region           = var.region
  schedule         = var.scheduler_schedules[each.key]
  time_zone        = "Etc/UTC"
  attempt_deadline = "320s"
  depends_on       = [google_project_service.required]

  retry_config {
    retry_count          = 3
    min_backoff_duration = "30s"
    max_backoff_duration = "300s"
    max_doublings        = 3
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.worker[each.key].name}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
}

resource "google_logging_metric" "runtime_errors" {
  name        = "agent_economy_runtime_errors"
  description = "Cloud Run service and job errors for Agent Economy Monitor"
  depends_on  = [google_project_service.required]
  filter      = <<-EOT
    (resource.type="cloud_run_revision" OR resource.type="cloud_run_job")
    AND severity>=ERROR
    AND (resource.labels.service_name="agent-economy-monitor"
      OR resource.labels.job_name=~"agent-economy-.*")
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}

resource "google_monitoring_alert_policy" "runtime_errors" {
  display_name = "Agent Economy Monitor runtime errors"
  combiner     = "OR"
  depends_on   = [google_project_service.required]

  conditions {
    display_name = "Any runtime error in five minutes"
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.runtime_errors.name}\""
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_DELTA"
      }
    }
  }

  notification_channels = var.notification_channels
}

resource "google_monitoring_alert_policy" "database_cpu" {
  display_name = "Agent Economy Monitor Cloud SQL CPU"
  combiner     = "OR"
  depends_on   = [google_project_service.required]

  conditions {
    display_name = "Cloud SQL CPU above 70 percent for 15 minutes"
    condition_threshold {
      filter          = "resource.type=\"cloudsql_database\" AND metric.type=\"cloudsql.googleapis.com/database/cpu/utilization\""
      duration        = "900s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0.70

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  notification_channels = var.notification_channels
}

resource "google_billing_budget" "monthly" {
  billing_account = var.billing_account
  display_name    = "Agent Economy Monitor monthly infrastructure"
  depends_on      = [google_project_service.required]

  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.monthly_budget_amount)
    }
  }

  budget_filter {
    projects = ["projects/${data.google_project.current.number}"]
  }

  threshold_rules {
    threshold_percent = 0.50
  }
  threshold_rules {
    threshold_percent = 0.80
  }
  threshold_rules {
    threshold_percent = 1.00
  }
}
